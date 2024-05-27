package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/gocarina/gocsv"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Neo4j struct {
	Database string
	Uri      string
	NoAuth   bool
	Driver   neo4j.DriverWithContext
	Session  neo4j.SessionWithContext
}

type subjectObjectRelationCsv struct {
	Subject     string `csv:"Subject"` // struct tags are required for gocsv
	SubjectType string `csv:"SubjectType"`
	Relation    string `csv:"Relation"`
	Object      string `csv:"Object"`
	ObjectType  string `csv:"ObjectType"`
}

func init() {
	// Register our new output with benthos.
	configSpec := service.NewConfigSpec().
		Description("").
		Field(service.NewInterpolatedStringField("Database")).
		Field(service.NewInterpolatedStringField("Uri")).
		Field(service.NewBoolField("NoAuth"))

	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
		database, _ := conf.FieldString("Database")
		uri, _ := conf.FieldString("Uri")
		noAuth, _ := conf.FieldBool("NoAuth")

		return &Neo4j{Database: database, Uri: uri, NoAuth: noAuth}, 1, nil
	}

	err := service.RegisterOutput("cypher", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

func (neo *Neo4j) Connect(ctx context.Context) error {

	driver, err := neo4j.NewDriverWithContext(neo.Uri, neo4j.NoAuth())
	if err != nil {
		return err
	}
	neo.Driver = driver

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	neo.Session = session

	return nil
}

func (neo *Neo4j) Write(ctx context.Context, msg *service.Message) error {
	content, err := msg.AsStructuredMut()
	if err != nil {
		return err
	}

	collateTriples := content.(map[string]interface{})["SOR"].(string)

	SORs := []*subjectObjectRelationCsv{}
	gocsv.UnmarshalString(collateTriples, &SORs)

	for _, SOR := range SORs {
		_, err = neo.gdb_create_node(ctx, SOR.Subject, SOR.SubjectType)
		_, err = neo.gdb_create_node(ctx, SOR.Object, SOR.ObjectType)
		_, err = neo.gdb_create_relation(ctx, SOR.Subject, SOR.SubjectType, SOR.Object, SOR.ObjectType, SOR.Relation)
	}

	return nil
}

func (neo *Neo4j) Close(ctx context.Context) error {
	neo.Driver.Close(ctx)
	neo.Session.Close(ctx)
	return nil
}

func (neo *Neo4j) gdb_create_relation(ctx context.Context, subject_name string, subject_type string, object_name string, object_type string, relation_type string) (any, error) {

	_, err := neo.Session.ExecuteWrite(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
		result, err := transaction.Run(ctx, "MATCH (n:"+subject_type+"), (m:"+object_type+") WHERE n.name = '"+subject_name+"' AND m.name = '"+object_name+"' MERGE (n)-[l:"+relation_type+"]->(m)", map[string]any{"message": "hello, world"})
		if err != nil {
			return nil, err
		}

		if result.Next(ctx) {
			return result.Record().Values[0], nil
		}

		return nil, result.Err()
	})

	return nil, err
}

func (neo *Neo4j) gdb_create_node(ctx context.Context, subject_name string, subject_type string) (any, error) {

	_, err := neo.Session.ExecuteWrite(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
		result, err := transaction.Run(ctx, "MERGE (n:"+subject_type+" {name: '"+subject_name+"'})", map[string]any{"message": "hello, world"})
		if err != nil {
			return nil, err
		}

		if result.Next(ctx) {
			return result.Record().Values[0], nil
		}

		return nil, result.Err()
	})

	return nil, err

}
