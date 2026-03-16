package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/Role1776/gigago"
	_ "github.com/denisenkom/go-mssqldb"
)

// --- Configuration ---

// Config хранит конфигурацию приложения.
type Config struct {
	DBConnString    string
	GigaChatAuthKey string
	TargetSchema    string
}

// LoadConfig загружает конфигурацию из переменных окружения.
func LoadConfig() (*Config, error) {
	connStr := os.Getenv("DB_CONNECTION_STRING")
	if connStr == "" {
		// Fallback для примера, в продакшене лучше строго требовать ENV
		connStr = "server=serkweb.serk.lan;user id=sa;password=OrchestraSQL;port=1433;database=UserModules"
	}

	authKey := os.Getenv("GIGACHAT_AUTH_KEY")
	if authKey == "" {
		authKey = "MDE5YWEyMTMtNTAwYS03Nzk3LWFlNjQtNjZiZDVhNGM2YWFhOjQ0ODc5ZjE4LTU1MzgtNGVhNS1hNmRjLTE3ZmFlMzI2Y2IxZA=="
	}

	return &Config{
		DBConnString:    connStr,
		GigaChatAuthKey: authKey,
		TargetSchema:    "user", // Значение по умолчанию
	}, nil
}

// --- Domain Models ---

// TableColumns представляет группировку полей по таблицам.
type TableColumns map[string][]string

// --- Database Repository ---

// DBRepository инкапсулирует логику работы с базой данных.
type DBRepository struct {
	db *sql.DB
}

func NewDBRepository(db *sql.DB) *DBRepository {
	return &DBRepository{db: db}
}

// GetTableColumns retrieves columns missing descriptions.
func (r *DBRepository) GetTableColumns(ctx context.Context, dbName, schema string) (TableColumns, error) {
	query := `
        SELECT TABLE_NAME, COLUMN_NAME 
        FROM usermodules.[user].ALL_DB_TB_COL_LIST 
        WHERE DATABASE_NAME = @p1 AND (DESCRIPTION IS NULL OR DESCRIPTION = '') AND SCHEMA_NAME=@p2`

	rows, err := r.db.QueryContext(ctx, query, dbName, schema)
	if err != nil {
		return nil, fmt.Errorf("query columns: %w", err)
	}
	defer rows.Close()

	tables := make(TableColumns)
	for rows.Next() {
		var tableName, columnName string
		if err := rows.Scan(&tableName, &columnName); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		// Очистка имени колонки от непечатных символов
		cleanName := strings.TrimFunc(columnName, func(r rune) bool { return !unicode.IsPrint(r) })
		tables[tableName] = append(tables[tableName], cleanName)
	}

	return tables, rows.Err()
}

// UpdateMetadata updates the central metadata table.
func (r *DBRepository) UpdateMetadata(ctx context.Context, dbName, tableName string, descriptions map[string]string) error {
	const updateQuery = `
        UPDATE usermodules.[user].ALL_DB_TB_COL_LIST SET DESCRIPTION = @p1 
        WHERE DATABASE_NAME = @p2 AND TABLE_NAME = @p3 AND COLUMN_NAME = @p4`

	for col, desc := range descriptions {
		if _, err := r.db.ExecContext(ctx, updateQuery, desc, dbName, tableName, col); err != nil {
			return fmt.Errorf("update metadata for %s.%s: %w", tableName, col, err)
		}
	}
	return nil
}

// UpdateExtendedProperties updates MS SQL extended properties (sp_add/updateextendedproperty).
func (r *DBRepository) UpdateExtendedProperties(ctx context.Context, dbName, tableName string, descriptions map[string]string, schema string) error {
	for col, desc := range descriptions {
		// Сначала пытаемся обновить. Если строки нет (ошибка), добавляем.
		err := r.tryUpdateExtendedProperty(ctx, dbName, tableName, col, desc, schema)
		if err != nil {
			// Если ошибка говорит о том, что свойства нет, пробуем добавить
			if isPropertyNotFoundError(err) {
				if addErr := r.addExtendedProperty(ctx, dbName, tableName, col, desc, schema); addErr != nil {
					return fmt.Errorf("add extended property failed: %w", addErr)
				}
				continue
			}
			return fmt.Errorf("update extended property failed: %w", err)
		}
	}
	return nil
}

func (r *DBRepository) tryUpdateExtendedProperty(ctx context.Context, dbName, tableName, col, desc, schema string) error {
	// Используем quoteIdentifier для защиты от инъекций в именах объектов
	query := fmt.Sprintf(`
        USE %s;
        EXEC sp_updateextendedproperty
            @name = N'MS_Description',
            @value = @p1,
            @level0type = N'SCHEMA', @level0name = %s,
            @level1type = N'TABLE', @level1name = @p2,
            @level2type = N'COLUMN', @level2name = @p3`,
		quoteIdentifier(dbName), quoteString(schema))

	_, err := r.db.ExecContext(ctx, query, desc, tableName, col)
	return err
}

func (r *DBRepository) addExtendedProperty(ctx context.Context, dbName, tableName, col, desc, schema string) error {
	query := fmt.Sprintf(`
        USE %s;
        EXEC sp_addextendedproperty
            @name = N'MS_Description',
            @value = @p1,
            @level0type = N'SCHEMA', @level0name = %s,
            @level1type = N'TABLE', @level1name = @p2,
            @level2type = N'COLUMN', @level2name = @p3`,
		quoteIdentifier(dbName), quoteString(schema))

	_, err := r.db.ExecContext(ctx, query, desc, tableName, col)
	return err
}

// --- AI Service ---

// AIService handles interaction with GigaChat.
type AIService struct {
	client *gigago.Client
	model  *gigago.GenerativeModel
}

func NewAIService(authKey string) (*AIService, error) {
	// Используем контекст с таймаутом для инициализации
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := gigago.NewClient(ctx,
		authKey,
		gigago.WithCustomInsecureSkipVerify(true),
		gigago.WithCustomTimeout(600*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("create AI client: %w", err)
	}

	model := client.GenerativeModel("GigaChat")
	model.SystemInstruction = `Ты — администратор базы данных. Твоя задача - обновить описание полей в таблицах базы данных.`
	model.Temperature = 0.87

	return &AIService{client: client, model: model}, nil
}

func (s *AIService) Close() {
	if s.client != nil {
		s.client.Close()
	}
}

func (s *AIService) GenerateDescription(ctx context.Context, dbName, dbDesc, tableName string, columns []string) (map[string]string, error) {
	request := fmt.Sprintf("%s;%s;%s;%s", dbName, dbDesc, tableName, strings.Join(columns, ";"))

	messages := []gigago.Message{
		{
			Role: gigago.RoleUser,
			Content: `По названию базы данных, описанию назначения данных в этой базе, названиям таблицы и поля в таблице придумать описание для этого поля на английском языке. 
            Входные данные организованы так, разделитель - точка с запятой:
            название базы данных;назначение данных;название таблицы;список полей
            Формат ответа - список название поля:описание поля разделённыем точкой с запятой, одной строкой. Не используй сивол перевода строки.
            Описание должно быть на английском языке.
            Входные данные:` + request,
		},
	}

	resp, err := s.model.Generate(ctx, messages)
	if err != nil {
		return nil, fmt.Errorf("generate ai response: %w", err)
	}

	if len(resp.Choices) == 0 {
		return nil, errors.New("empty ai response choices")
	}

	return parseAIResponse(resp.Choices[0].Message.Content), nil
}

// parseAIResponse парсит ответ AI в мапу.
func parseAIResponse(response string) map[string]string {
	descriptions := make(map[string]string)
	parts := strings.Split(response, ";")
	for _, part := range parts {
		kv := strings.SplitN(part, ":", 2)
		if len(kv) == 2 {
			key := strings.TrimSpace(kv[0])
			val := strings.TrimSpace(kv[1])
			if key != "" {
				descriptions[key] = val
			}
		}
	}
	return descriptions
}

// --- Main Application Logic ---

func run(ctx context.Context, cfg *Config, dbName, dbDesc string) error {
	// Init DB
	db, err := sql.Open("sqlserver", cfg.DBConnString)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}
	slog.Info("Connected to database")

	// Init AI
	ai, err := NewAIService(cfg.GigaChatAuthKey)
	if err != nil {
		return fmt.Errorf("init ai: %w", err)
	}
	defer ai.Close()

	repo := NewDBRepository(db)

	// 1. Fetch missing columns
	slog.Info("Fetching columns...", "db", dbName)
	tables, err := repo.GetTableColumns(ctx, dbName, cfg.TargetSchema)
	if err != nil {
		return err
	}

	// 2. Process each table
	for tableName, columns := range tables {
		slog.Info("Processing table", "table", tableName, "columns", len(columns))

		// Generate AI descriptions
		// Добавляем небольшой таймаут на генерацию
		genCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
		descs, err := ai.GenerateDescription(genCtx, dbName, dbDesc, tableName, columns)
		cancel()

		if err != nil {
			slog.Error("Failed to generate description", "table", tableName, "error", err)
			continue
		}

		// Update Extended Properties
		if err := repo.UpdateExtendedProperties(ctx, dbName, tableName, descs, cfg.TargetSchema); err != nil {
			slog.Error("Failed to update extended properties", "table", tableName, "error", err)
		}

		// Update Metadata Table
		if err := repo.UpdateMetadata(ctx, dbName, tableName, descs); err != nil {
			slog.Error("Failed to update metadata table", "table", tableName, "error", err)
		}
	}

	slog.Info("Processing finished")
	return nil
}

func main() {
	// Setup Logger (JSON format for production, Text for local)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Context with cancellation
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Load Config
	cfg, err := LoadConfig()
	if err != nil {
		slog.Error("Config load failed", "error", err)
		os.Exit(1)
	}

	// Parse Args
	if len(os.Args) < 3 {
		slog.Error("Usage: go run main.go <db_name> <db_description>")
		os.Exit(1)
	}
	databaseName := os.Args[1]
	databaseDescription := os.Args[2]

	if err := run(ctx, cfg, databaseName, databaseDescription); err != nil {
		slog.Error("Application error", "error", err)
		os.Exit(1)
	}
}

// --- Helpers ---

// quoteIdentifier protects against SQL injection for object names (bracket escaping)
func quoteIdentifier(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

func quoteString(name string) string {
	return "'" + strings.ReplaceAll(name, "'", "''") + "'"
}

// isPropertyNotFoundError checks specific SQL Server error for missing extended property
func isPropertyNotFoundError(err error) bool {
	// В реальном коде стоит проверять конкретный код ошибки mssql (например, код 15133 или подобный)
	// Для упрощения проверяем строку, если драйвер возвращает её
	return strings.Contains(err.Error(), "property cannot be found") || strings.Contains(err.Error(), "does not exist")
}
