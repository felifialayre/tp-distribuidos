package parquet_row

var ParquetColumns = []string{
	"abstract",
	"additional_entities",
	"date_created",
	"date_modified",
	"description",
	"event",
	"identifier",
	"image",
	"in_language",
	"infoboxes",
	"is_part_of",
	"license",
	"main_entity",
	"name",
	"references",
	"sections",
	"tables",
	"url",
	"version",
}

type ParquetRow struct {
	Abstract string
	AdditionalEntities string
	DateCreated string
	DateModified string
	Description string
	Event string
	Identifier int64
	Image string
	InLanguage string
	Infoboxes string
	IsPartOf string
	License string
	MainEntity string
	Name string
	References string
	Sections string
	Tables string
	URL string
	Version string
}