
open ERAP

[<EntryPoint>]
let main _ =
    Database.connectionCreate ()
    |> Database.init
    |> ignore

    Api.start()
    0 // return an integer exit code