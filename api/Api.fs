
namespace ERAP

open Microsoft.AspNetCore
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Http
open Microsoft.Extensions.DependencyInjection
open Giraffe
open Giraffe.EndpointRouting
open FSharp.Control.Tasks
open System

module Api =
    module Tenant =
        let create : HttpHandler =
            fun (next:HttpFunc) (ctx:HttpContext) -> task {
                try
                    let tenant = ctx.BindModelAsync<Tenant>()
                    ctx.SetStatusCode(202)
                    return! json tenant next ctx
                with e ->
                    ctx.SetStatusCode(406)
                    return! ctx.WriteTextAsync(e.Message)
            }

        let update : HttpHandler = 
            fun (next:HttpFunc) (ctx:HttpContext) -> task {
                try
                    let tenant = ctx.BindModelAsync<Tenant>()
                    ctx.SetStatusCode(202)
                    return! json tenant next ctx
                with e ->
                    ctx.SetStatusCode(406)
                    return! ctx.WriteTextAsync(e.Message)  
            }

    module IncomeProxy =
        let check (checkid:string) : HttpHandler =
            fun (_:HttpFunc) (ctx:HttpContext) -> task {
                match Guid.TryParse(checkid) with 
                | true, id ->
                    ctx.SetStatusCode(202)
                    return! ctx.WriteTextAsync($"{id} is a valid CheckID!")
                | false, failed -> 
                    ctx.SetStatusCode(406)
                    return! ctx.WriteTextAsync($"{failed} is not a valid CheckID")  
            }

    module Report =
        let create : HttpHandler =
            fun (next:HttpFunc) (ctx:HttpContext) -> task {
                try
                    let report = ctx.BindModelAsync<Report>()
                    ctx.SetStatusCode(202)
                    return! json report next ctx
                with e ->
                    ctx.SetStatusCode(406)
                    return! ctx.WriteTextAsync(e.Message)
            }
        
        let update : HttpHandler =
            fun (next:HttpFunc) (ctx:HttpContext) -> task {
                try
                    let report = ctx.BindModelAsync<Report>()
                    ctx.SetStatusCode(202)
                    return! json report next ctx
                with e ->
                    ctx.SetStatusCode(406)
                    return! ctx.WriteTextAsync(e.Message)
            }

        let view (reportid: string) : HttpHandler =
            fun (_:HttpFunc) (ctx:HttpContext) -> task {
                match Guid.TryParse(reportid) with 
                | true, id ->
                    ctx.SetStatusCode(202)
                    return! ctx.WriteTextAsync($"{id} is a valid reportid!")
                | false, failed -> 
                    ctx.SetStatusCode(406)
                    return! ctx.WriteTextAsync($"{failed} is not a valid reportid")
            }



     module Grantee =
        let create : HttpHandler =
            fun (next:HttpFunc) (ctx:HttpContext) -> task {
                try
                    let grantee = ctx.BindModelAsync<Grantee>()
                    ctx.SetStatusCode(202)
                    return! json grantee next ctx
                with e ->
                    ctx.SetStatusCode(406)
                    return! ctx.WriteTextAsync(e.Message)
            }

        let update : HttpHandler =
            fun (next:HttpFunc) (ctx:HttpContext) -> task {
                try
                    let grantee = ctx.BindModelAsync<Grantee>()
                    ctx.SetStatusCode(202)
                    return! json grantee next ctx
                with e ->
                    ctx.SetStatusCode(406)
                    return! ctx.WriteTextAsync(e.Message)
            }

        let view (granteeid:string) : HttpHandler =
            fun (_:HttpFunc) (ctx:HttpContext) -> task {
                match Guid.TryParse(granteeid) with 
                | true, id ->
                    ctx.SetStatusCode(202)
                    return! ctx.WriteTextAsync($"{id} is a valid granteeid!")
                | false, failed -> 
                    ctx.SetStatusCode(406)
                    return! ctx.WriteTextAsync($"{failed} is not a valid granteeid")  
            }

    module Grant =
        let create : HttpHandler =
            fun (next:HttpFunc) (ctx:HttpContext) -> task {
                try
                    let grant = ctx.BindModelAsync<Grantee>()
                    ctx.SetStatusCode(202)
                    return! json grant next ctx
                with e ->
                    ctx.SetStatusCode(406)
                    return! ctx.WriteTextAsync(e.Message)
            }

        let update : HttpHandler =
            fun (next:HttpFunc) (ctx:HttpContext) -> task {
                try
                    let grant = ctx.BindModelAsync<Grantee>()
                    ctx.SetStatusCode(202)
                    return! json grant next ctx
                with e ->
                    ctx.SetStatusCode(406)
                    return! ctx.WriteTextAsync(e.Message)
            }

        let view (grantid:string) : HttpHandler =
            fun (_:HttpFunc) (ctx:HttpContext) -> task {
                match Guid.TryParse(grantid) with 
                | true, id ->
                    ctx.SetStatusCode(202)
                    return! ctx.WriteTextAsync($"{id} is a valid CheckID!")
                | false, failed -> 
                    ctx.SetStatusCode(406)
                    return! ctx.WriteTextAsync($"{failed} is not a valid CheckID")  
            }
                
    let routing =
        [
            POST [ route "/grantee" Grantee.create ]
            PUT [ route "/grantee" Grantee.update ]
            GET [ routef "/grantee/%s" Grantee.view ]
            
            POST [ route "/reporting" Report.create ]
            PUT [ route "/reporting" Report.update]
            GET [ routef "/reporting/%s" Report.view ]

            POST [ route "/tenant" Tenant.create]
            PUT [route "/tenant" Tenant.update]
            GET [ routef "/incomeproxy/%s" IncomeProxy.check ]

            POST [ route "/grant" Grant.create ]
            PUT [ route "/grant" Grant.update ]
            GET [ routef "/grant/%s" Grant.view ]
        ]

    let notFoundHandler =
        text "Not Found" 
        |> RequestErrors.notFound

    let configureApp (appBuilder : IApplicationBuilder) =
        appBuilder
            .UseRouting()
            .UseGiraffe(routing)
            .UseGiraffe(notFoundHandler)

    let configureServices (services : IServiceCollection) =
        services
            .AddRouting()
            .AddGiraffe()
        |> ignore

    let start () =
        WebHost
            .CreateDefaultBuilder()
            .UseKestrel()
            .Configure(configureApp)
            .ConfigureServices(configureServices)
            .Build()
            .Run()