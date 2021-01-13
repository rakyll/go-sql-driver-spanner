package spannerdriver

import (
	"testing"
	"os"
	"context"
	"log"
	"database/sql"
	"runtime/debug"
	"fmt"
	"reflect"

	"cloud.google.com/go/spanner"

	// api/lib packages not imported by driver
	"google.golang.org/grpc"
	"google.golang.org/api/option"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"

	_ "github.com/rakyll/go-sql-driver-spanner"

)

var(
	project string
	instance string
	dbname string
	dsn string
)


// connector things 
type Connector struct {
	ctx         context.Context	
	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient
}

func NewConnector()(*Connector, error){

	ctx := context.Background()

	adminClient, err := adminapi.NewDatabaseAdminClient(
		ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint("0.0.0.0:9010"),
		option.WithGRPCDialOption(grpc.WithInsecure()))
	if err != nil {
			return nil, err
	}

	dataClient, err := spanner.NewClient(ctx, dsn)
	if err != nil {
			return nil,err
	}
	curs := &Connector{
		ctx: ctx,
		client: dataClient,
		adminClient: adminClient,

	}
	return curs,nil
}

func (c *Connector) Close() {
	c.client.Close()
	c.adminClient.Close()
}

// structures for row data 
type testaRow struct{
	A string
	B string
	C string
}
type typeTestaRow struct {
	stringt string 
	intt int 
	floatt float64
	boolt bool
}


func init(){

	// get environment variables
	instance = os.Getenv("SPANNER_TEST_INSTANCE")
	project = os.Getenv("SPANNER_TEST_PROJECT")
	dbname = os.Getenv("SPANNER_TEST_DBNAME")

	// set defaults if none provided 
	if instance == "" {instance = "test-instance" }
	if project == "" { project = "test-project" }
	if dbname == "" { dbname = "gotest" }

	// derive data source name 
	dsn = "projects/" + project + "/instances/" + instance + "/databases/" + dbname

}


// helper funs //

// functions that use the client lib / apis ~ 
// ******************* //

// Executes DDL statements 
// !!! adminpb/ genproto is an experimenal repo
// duct tape
func executeDdlApi(curs *Connector, ddls []string){

	op, err := curs.adminClient.UpdateDatabaseDdl(curs.ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   dsn,
		Statements: ddls,
	})
	if err != nil {
		//return nil, err
		log.Fatal(err)
	}
	if err := op.Wait(curs.ctx); err != nil {
		//return nil, err
		log.Fatal(err)
	}
}

 
// duct tape
func ExecuteDMLClientLib(dml []string){

	// open client
	var db = "projects/"+project+"/instances/"+instance+"/databases/"+dbname;
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
			log.Fatal(err)
	}
	defer client.Close()

	// Put strings into spanner.Statement structure
	var states []spanner.Statement
	for _,line := range dml {
		states = append(states, spanner.NewStatement(line))
	}

	// execute statements
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmts := states
		rowCounts, err := txn.BatchUpdate(ctx, stmts)
		if err != nil {
				return err
		}
		fmt.Printf("Executed %d SQL statements using Batch DML.\n", len(rowCounts))
		return nil
		})
	if (err != nil) { log.Fatal(err) }
}

// end client lib funs 
// ******************* //


// helper funs for tests //

func mustExecContext(t * testing.T, ctx context.Context, db *sql.DB, query string){}

func mustQueryContext( t *testing.T, ctx context.Context, db *sql.DB, query string) (rows *sql.Rows){return nil}


//  #### tests ####  // 

// Tests general query functionality 
func TestQueryBasic(t *testing.T){

	// set up test table
	curs, err := NewConnector()
	if err != nil{
		log.Fatal(err)
	}

	executeDdlApi(curs, []string{`CREATE TABLE Testa (
		A   STRING(1024),
		B  STRING(1024),
		C   STRING(1024)
	)	 PRIMARY KEY (A)`}) // duct tape 

	ExecuteDMLClientLib([]string{`INSERT INTO Testa (A, B, C) 
		VALUES ("a1", "b1", "c1"), ("a2", "b2", "c2") , ("a3", "b3", "c3") `}) // duct tape 

	// open db 
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}

	// run unit tests 
	EmptyQuery(t, db, ctx)
	SyntaxErrorQuery(t, db, ctx)
	ReturnNothingrQuery(t, db, ctx)
	OneTupleQuery(t, db, ctx)
	SubsetQuery(t, db, ctx)
	WholeTableQuery(t, db, ctx)
	ColSubseteQuery(t, db, ctx)

	// clear table 
	//executeDdlApi(curs, []string{`DROP TABLE Testa`})

	// close connection 
	curs.Close()
	db.Close()
}

// sql unit tests //

// send empty string as query 
func EmptyQuery(t *testing.T, db *sql.DB, ctx context.Context){
	rows, err := db.QueryContext(ctx, "")
	if err != nil {
		t.Error(err.Error()) // doesn't err, just prints to stdout
	}

	numRows := 0
	for rows.Next(){
		numRows ++
	}
	rows.Close()

	if numRows != 0 {
		t.Errorf("Shouldn't return any rows")
	}
}

// seend query with sql syntax error 
func SyntaxErrorQuery(t *testing.T, db *sql.DB, ctx context.Context){
	rows, err := db.QueryContext(ctx, "SELECT SELECT * FROM Testa")

	if err != nil {
		t.Errorf(err.Error()) // doesn't err, just prints to stdout
	}

	numRows := 0
	for rows.Next(){
		numRows ++
	}
	rows.Close()

	if numRows != 0 {
		t.Errorf("Shouldn't return any rows")
	}
}


// query that should return nothing 
func ReturnNothingrQuery(t *testing.T, db *sql.DB, ctx context.Context){

}

// query that should return one tuple
func OneTupleQuery(t *testing.T, db *sql.DB, ctx context.Context){}

// should return two tuples
func SubsetQuery(t *testing.T, db *sql.DB, ctx context.Context){

	want := []testaRow{
		{A:"a1", B:"b1", C:"c1"},
		{A:"a2", B:"b2", C:"c2"},
	}
	
	rows, err := db.QueryContext(ctx, "SELECT * FROM Testa WHERE A = \"a1\" OR A = \"a2\"")
	if err != nil {
		t.Errorf(err.Error())
	}

	got := []testaRow{}
	numRows := 0
	for rows.Next(){
		curr := testaRow{A:"", B:"", C:""}
		if err := rows.Scan(&curr.A, &curr.B, &curr.C); err != nil {
			t.Error(err.Error())
		}
		got = append(got, curr)
		numRows ++
	}
	rows.Close()

	if ! reflect.DeepEqual(want,got) {
		t.Errorf("Unexpected tuples returned \n got: %#v\nwant: %#v", got, want)
	}
}

// should return entire table
func WholeTableQuery(t *testing.T, db *sql.DB, ctx context.Context){

	want := []testaRow{
		{A:"a1", B:"b1", C:"c1"},
		{A:"a2", B:"b2", C:"c2"},
		{A:"a3", B:"b3", C:"c3"},
	}

	rows, err := db.QueryContext(ctx, "SELECT * FROM Testa ORDER BY A")
	if err != nil {
		t.Errorf(err.Error())
	}

	got := []testaRow{}
	numRows := 0
	for rows.Next(){
		curr := testaRow{A:"", B:"", C:""}
		if err := rows.Scan(&curr.A, &curr.B, &curr.C); err != nil {
			t.Error(err.Error())
		}
		got = append(got, curr)
		numRows ++
	}
	rows.Close()

	if ! reflect.DeepEqual(want,got) {
		t.Errorf("Unexpected tuples returned \n got: %#v\nwant: %#v", got, want)
	}
}

// Should return subset of columns
func ColSubseteQuery(t *testing.T, db *sql.DB, ctx context.Context){}


// tests atomic spanner types 
func TestQueryAtomicTypes( t *testing.T){}

// type unit tests // 

// check that atomic types read in as expected 
func GeneralAtomicTypeQuery(t *testing.T, db *sql.DB, ctx context.Context){}

// check behavior of NaN, +Inf, -Inf
func SpecialFloatTypeQuery(t *testing.T, db *sql.DB, ctx context.Context){}