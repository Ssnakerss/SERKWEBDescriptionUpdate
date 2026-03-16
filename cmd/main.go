package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"serkwebDescriptionUpdate/internal/config"
	"strings"
	"time"
	"unicode"

	"github.com/Role1776/gigago"
	_ "github.com/denisenkom/go-mssqldb"
)

// Call AI agent
func generateFieldDescriptionByAI(ctx context.Context, key string, request string) (response string, err error) {
	time.Sleep(3000 * time.Millisecond)
	client, err := gigago.NewClient(ctx,
		key,
		gigago.WithCustomInsecureSkipVerify(true),
		gigago.WithCustomTimeout(120*time.Second),
	)
	if err != nil {
		log.Fatalf("Ошибка создания клиента %v", err)
	}

	defer client.Close()

	// Получаем модель, с которой будем работать.
	model := client.GenerativeModel("GigaChat")

	// (Опционально) Настраиваем параметры модели.
	// Можно задать системный промпт, температуру и другие параметры.
	model.SystemInstruction = `Ты — администратор базы данных. Твоя задача - обновить описание полей в таблицах базы данных.
	`
	model.Temperature = 0.87

	// Формируем сообщение для отправки.
	messages := []gigago.Message{
		{Role: gigago.RoleUser,
			Content: `По названию базы данных, описанию назначения данных в этой базе, названиям таблицы и поля в таблице придумать описание для этого поля на английском языке. 
			Входные данные организованы так, разделитель-точка с запятой:
			название базы данных;назначение данных;название таблицы;список полей
			Например: Watchdog;система мониторинга работы оборудования;Machines;MachineName;MachineType;MACAddress
			Формат ответа - список название поля:описание поля разделённыем точкой с запятой, одной строкой. Не используй сивол перевода строки.
			не изменяй названия полей.
			описание должно быть на английском языке.
			Например - MachineName:The name of the machine/device;MachineType:The type of the device;MACAddress:The MAC address of the machine
			Входные данные:` + request,
		},
	}

	// Отправляем запрос и получаем ответ.
	resp, err := model.Generate(ctx, messages)
	if err != nil {
		return "", err
	}
	return resp.Choices[0].Message.Content, nil
}

// generateFieldDescriptionsForTable возвращает map с описаниями для всех полей указанной таблицы
func generateFieldDescriptionsForTable(cfg *config.Config, tableName string, columns []string) (map[string]string, error) {
	request := cfg.Database + ";" + cfg.DataDescription + ";" + tableName
	for _, column := range columns {
		request += ";" + column
		// fieldDescriptions[column] = tableName + ":" + generateRandomDescription()
	}
	response, err := generateFieldDescriptionByAI(context.Background(), cfg.GigaChatAuthKey, request)
	if err != nil {
		return nil, err
	}
	fieldDescriptions := make(map[string]string)
	resp := strings.Split(response, ";")
	for _, r := range resp {
		col := strings.Split(r, ":")
		if len(col) == 2 {
			cName := strings.TrimFunc(col[0], func(r rune) bool { return !unicode.IsPrint(r) })
			cDesc := strings.TrimFunc(col[1], func(r rune) bool { return !unicode.IsPrint(r) })
			if cName != "" {
				fieldDescriptions[cName] = cDesc
			}
		}
	}
	return fieldDescriptions, nil
}

// updateTableColumnDescriptions обновляет описания полей в таблице ALL_DB_TB_COL_LIST для указанной базы данных
func updateTableColumnDescriptions(db *sql.DB, cfg *config.Config) error {
	// Получаем список полей (TABLE_NAME, COLUMN_NAME) для указанной базы данных
	query := `SELECT TABLE_NAME, COLUMN_NAME 
	FROM usermodules.[user].ALL_DB_TB_COL_LIST 
	WHERE DATABASE_NAME = @p1 AND (DESCRIPTION IS NULL OR DESCRIPTION = '') AND SCHEMA_NAME='` + cfg.Schema + `'`
	rows, err := db.Query(query, cfg.Database)
	if err != nil {
		return fmt.Errorf("ошибка выполнения запроса: %w", err)
	}
	defer rows.Close()

	// Группируем поля по таблицам
	tableColumns := make(map[string][]string)
	for rows.Next() {
		var tableName, columnName string
		if err := rows.Scan(&tableName, &columnName); err != nil {
			return fmt.Errorf("ошибка чтения строки: %w", err)
		}
		columnName = strings.TrimFunc(columnName, func(r rune) bool { return !unicode.IsPrint(r) })
		tableColumns[tableName] = append(tableColumns[tableName], columnName)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("ошибка при итерации по строкам: %w", err)
	}

	// Для каждой таблицы генерируем описания и обновляем
	for tableName, columns := range tableColumns {
		fmt.Printf("Работаем с таблицей %s\n", tableName)
		fieldDescriptions, err := generateFieldDescriptionsForTable(cfg, tableName, columns)
		if err != nil {
			return err
		}
		err = updateDescriptionDb(db, cfg, tableName, fieldDescriptions)
		if err != nil {
			log.Printf("Ошибка обновления описаний для таблицы %s: %v", tableName, err)
			// Продолжаем обработку других таблиц
		}
	}

	return nil
}

// updateDescriptionDb обновляет описания полей в самой таблице базы данных с помощью расширенных свойств
func updateDescriptionDb(db *sql.DB, cfg *config.Config, tableName string, fieldDescriptions map[string]string) error {
	for column, description := range fieldDescriptions {
		// Проверяем, существует ли уже описание (extended property) для столбца
		checkQuery := "USE " + cfg.Database + `
		SELECT 1 FROM fn_listextendedproperty(N'MS_Description',
			N'SCHEMA', N'` + cfg.Schema + `',
			N'TABLE', @p1,
			N'COLUMN', @p2)`

		rows, err := db.Query(checkQuery, tableName, column)
		if err != nil {
			log.Printf("Ошибка проверки описания для %s.%s.%s: %v", cfg.Database, tableName, column, err)
			continue
		}

		var exists bool
		if rows.Next() {
			exists = true
		}
		rows.Close()

		var execQuery string
		if exists {
			// Обновляем существующее описание
			execQuery = "USE " + cfg.Database + `
			EXEC sp_updateextendedproperty
				@name = N'MS_Description',
				@value = @p1,
				@level0type = N'SCHEMA', @level0name = N'` + cfg.Schema + `',
				@level1type = N'TABLE', @level1name = @p2,
				@level2type = N'COLUMN', @level2name = @p3`
		} else {
			// Добавляем новое описание
			execQuery = "USE " + cfg.Database + `
			EXEC sp_addextendedproperty
				@name = N'MS_Description',
				@value = @p1,
				@level0type = N'SCHEMA', @level0name = N'` + cfg.Schema + `',
				@level1type = N'TABLE', @level1name = @p2,
				@level2type = N'COLUMN', @level2name = @p3`
		}
		_, err = db.Exec(execQuery, description, tableName, column)
		if err != nil {
			log.Printf("Ошибка установки описания для %s.%s.%s: ", cfg.Database, tableName, column)
			log.Printf("%v", err)
			log.Printf("Description: %s", description)
			log.Print("--------------------------")
			continue // Продолжаем, даже если одна операция не удалась
		}

		err = updateDescriptionINDb(db, cfg.Database, tableName, column, description)
		if err != nil {
			log.Printf("[ALL_DB_TB_COL_LIST] Ошибка обновления описания  для %s.%s.%s: %v", cfg.Database, tableName, column, err)
		}

	}
	return nil
}

// updateDescriptionDb обновляет описания указанных полей в таблице базы данных
func updateDescriptionINDb(db *sql.DB, databaseName, tableName, columnName, description string) error {
	updateQuery := `UPDATE usermodules.[user].ALL_DB_TB_COL_LIST SET 
			DESCRIPTION = @p1 
			WHERE 
			DATABASE_NAME = @p2 AND 
			TABLE_NAME = @p3 
			AND COLUMN_NAME = @p4`
	_, err := db.Exec(updateQuery, description, databaseName, tableName, columnName)
	return err
}

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatal("Ошибка загрузки конфигурации: ", err)
	}
	if cfg.Database == "" || cfg.DataDescription == "" || cfg.Schema == "" {
		log.Fatal("необходимо указать базу-database, схему-schems и описание данных-datadesc")
	}

	// Подключаемся к базе данных
	db, err := sql.Open("sqlserver", cfg.DBConnString)
	if err != nil {
		log.Fatal("Ошибка подключения к базе данных: ", err)
	}
	defer db.Close()

	// Проверка соединения
	if err := db.Ping(); err != nil {
		log.Fatal("Ошибка пинга базы данных: ", err)
	}

	// Обновляем описания
	if err := updateTableColumnDescriptions(db, cfg); err != nil {
		log.Fatal("Ошибка обновления описаний: ", err)
	}

	fmt.Printf("Описания для базы данных '%s' успешно обновлены!\n", cfg.Database)
}
