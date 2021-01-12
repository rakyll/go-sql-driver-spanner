package spannerdriver

import (
	"testing"
	"os"
	"context"
	//"log"
	"database/sql"
	//"runtime/debug"
	//"fmt"
	//"reflect"

	"cloud.google.com/go/spanner"

	// api/lib packages not imported by driver
	"google.golang.org/grpc"
	"google.golang.org/api/option"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	//adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"

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
func executeDdlApi(curs *Connector, ddls []string){}

 
// duct tape
func ExecuteDMLClientLib(dml []string){}

// end client lib funs 
// ******************* //


// helper funs for tests //

func mustExecContext(t * testing.T, ctx context.Context, db *sql.DB, query string){}

func mustQueryContext( t *testing.T, ctx context.Context, db *sql.DB, query string) (rows *sql.Rows){return nil}


//  #### tests ####  // 

// Tests general query functionality 
func TestQueryBasic(t *testing.T){}

// sql unit tests //

// send empty string as query 
func EmptyQuery(t *testing.T, db *sql.DB, ctx context.Context){}

// seend query with sql syntax error 
func SyntaxErrorQuery(t *testing.T, db *sql.DB, ctx context.Context){}


// query that should return nothing 
func ReturnNothingrQuery(t *testing.T, db *sql.DB, ctx context.Context){

}

// query that should return one tuple
func OneTupleQuery(t *testing.T, db *sql.DB, ctx context.Context){}

// should return two tuples
func SubsetQuery(t *testing.T, db *sql.DB, ctx context.Context){}

// should return entire table
func WholeTableQuery(t *testing.T, db *sql.DB, ctx context.Context){}

// Should return subset of columns
func ColSubseteQuery(t *testing.T, db *sql.DB, ctx context.Context){}


// tests atomic spanner types 
func TestQueryAtomicTypes( t *testing.T){}

// type unit tests // 

// check that atomic types read in as expected 
func GeneralAtomicTypeQuery(t *testing.T, db *sql.DB, ctx context.Context){}

// check behavior of NaN, +Inf, -Inf
func SpecialFloatTypeQuery(t *testing.T, db *sql.DB, ctx context.Context){}