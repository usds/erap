
namespace ERAP

open System
open System.Threading
// 🐘 POSTGRES CODE 🐘
open Npgsql.FSharp

module Database =
    
    [<Literal>]
    let private CONNECTION_STRING = "postgres://postgres:postgres@database/erap"
    let connectionCreate () = 
        Sql.connect(CONNECTION_STRING)

    let beginTransaction (connection:Sql.SqlProps) () =
        connection
        |> Sql.query """ 
            BEGIN
        """
        |> Sql.executeNonQuery
        |> ignore
    
    let commitTransaction (connection:Sql.SqlProps) () =
        connection
        |> Sql.query """ 
            COMMIT
        """
        |> Sql.executeNonQuery
        |> ignore

    let rec init (connection:Sql.SqlProps) =
        // We recursively try here because there is a race
        // condition where the database may not be finished 
        // connecting before we attempt to run our CREATE
        try
            connection
            |> Sql.query """ 
                CREATE TABLE IF NOT EXISTS ProxyChecks (
                    granteeid TEXT NOT NULL,
                    tenantid TEXT NOT NULL,
                    checkid TEXT NOT NULL PRIMARY KEY,
                    grantid TEXT,
                    datepaid TEXT,
                    dateapproved TEXT,
                    daterejected TEXT
                );
            """
            |> Sql.query """
                CREATE TABLE IF NOT EXISTS Grantees (
                    granteeid TEXT NOT NULL PRIMARY KEY,
                    zipcode TEXT NOT NULL,
                    state TEXT NOT NULL,
                    county TEXT NOT NULL,
                    city TEXT NOT NULL,
                    programname TEXT NOT NULL
                );
            """
            |> Sql.query """
                CREATE TABLE IF NOT EXISTS Tenants (
                    tenantid TEXT NOT NULL PRIMARY KEY,
                    zipcode TEXT NOT NULL,
                    addressline1 TEXT NOT NULL,
                    addressline2 TEXT NOT NULL,
                    county TEXT NOT NULL,
                    city TEXT NOT NULL,
                    firstname TEXT NOT NULL,
                    lastname TEXT NOT NULL,
                    annualincome DECIMAL NOT NULL,
                    notes TEXT NOT NULL
                );
            """
            |> Sql.query """
                CREATE TABLE IF NOT EXISTS Reports (
                    submittedon TEXT NOT NULL, 
                    granteeid TEXT NOT NULL,
                    reportid TEXT NOT NULL PRIMARY KEY,
                    queuetotal INTEGER NOT NULL,
                    paidoutcount INTEGER NOT NULL,
                    paidoutamount DECIMAL NOT NULL,
                    reportingperiodindays: INTEGER NOT NULL,
                    rejectedcount INTEGER NOT NULL,
                    version TEXT NOT NULL,
                    grantids TEXT NOT NULL []
                );
            """
            |> Sql.query """
                CREATE TABLE IF NOT EXISTS Grants (
                    name TEXT NOT NULL,
                    grantid TEXT NOT NULL PRIMARY KEY,
                    granteeid TEXT NOT NULL,
                    paidout DECIMAL NOT NULL,
                    balance DECIMAL NOT NULL
                );
            """
            |> Sql.executeNonQuery
            |> ignore
        with _ ->
            // 👼 Just take a lil nap 😴
            Thread.Sleep(500)
            init connection

    module Grantee =
        let get (connection:Sql.SqlProps) (granteeid:Guid) : Grantee list =
            connection
            |> Sql.query """
                SELECT * FROM Grantees WHERE granteeid = @granteeid
            """
            |> Sql.parameters(["granteeid", Sql.string (granteeid.ToString())])
            |> Sql.execute (fun read -> 
                {
                    ID = Some granteeid
                    Zipcode = read.text "zipcode"
                    County = read.text "county"
                    City = read.text "city"
                    State = read.text "state"
                    ProgramName = read.text "programname"
                }
            )

        let update (connection:Sql.SqlProps) (grantee:Grantee) : unit =
            beginTransaction connection ();
            connection
            |> Sql.query """
                UPDATE Grantees SET
                    city = @city,
                    county = @county,
                    programname = @programname,
                    state = @state, 
                    zipcode = @zipcode
                WHERE granteeid = @granteeid
            """
            |> Sql.parameters([
                "city", Sql.text grantee.City;
                "county", Sql.text grantee.County;
                "programname", Sql.text grantee.ProgramName;
                "state", Sql.text grantee.State;
                "zipcode", Sql.text grantee.Zipcode;
            ])
            |> Sql.executeNonQuery
            |> ignore
            commitTransaction connection ();