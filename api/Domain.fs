
namespace ERAP

open System

type Report = {
    SubmittedOn: DateTime
    Reporter: Grantee
    QueueTotal: int
    PaidOutCount: int
    PaidOutAmount: decimal
    ReportingPeriodinDays: int
    RejectedCount: int
    Version: string
    ID: Guid option
    GrantSources: (string * decimal) list
} and Grantee = {
    Zipcode: string
    County: string
    City: string
    State: string
    ID: Guid option
    ProgramName: string
} and IncomeProxy = {
    Grantee: Grantee
    Applicant: Tenant
    ID: Guid option
    GrantSource: string
    DatePaid: DateTime option
    DateApproved: DateTime option
    DateRejected: DateTime option
} and Tenant = {
    Zipcode: string
    AddressLine1: string
    AddressLine2: string
    State: string
    County: string
    City: string
    FirstName: string
    MiddleName: string
    LastName: string
    // TODO: Fully understand business logic
    AnnualIncome: decimal
    // Maybe have some structure
    // as opposed to ad-hoc text
    Notes: string
    ID: Guid option
} and ProxyResponse = 
    | Valid of IncomeProxy
    | NeedsMoreInformation of IncomeProxy * Message:string