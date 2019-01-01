

open System
open MySql
open MySqlConnector
open System.IO
open MySql.Data.MySqlClient

//获取配置文件信息
let getconig () =
    printfn "begin scan"
    let tmpfile=IO.File.ReadAllLines("./db.conf")
    tmpfile |> Array.map (fun x -> 
        x |> Console.WriteLine
        x ) |> Array.toList

let gettablename (conf:string) =
        conf.Split [|'='|] |>  Array.last
//获取对应表结构
let getDbStruct (conf:string,table_name:string)=
    async{
        use con= new MySqlConnection(conf)
        do! con.OpenAsync() |> Async.AwaitTask 
        use mutable commond = new MySqlCommand ()
        
        commond.Connection<-con
        commond.CommandText<- "select column_name from information_schema.columns where table_schema = @db and table_name = @tablename ;"
        con.Database |> Console.WriteLine
        commond.Parameters.AddWithValue("db",con.Database) |> ignore
        commond.Parameters.AddWithValue("tablename",table_name) |> ignore
        use! reader= commond.ExecuteReaderAsync() |> Async.AwaitTask
        let rec read()  =async{
                let! bl=reader.ReadAsync() |> Async.AwaitTask
                return match bl with
                        | true ->  
                                        let x = reader.GetString(0)
                                        let y=read() |>  Async.RunSynchronously
                                        List.append y [x]
                                        
                                        
                        | _ -> []
        }     
        return read() |>Async.RunSynchronously
    }
let printinfo (x:string) =
        let old= Console.ForegroundColor
        Console.ForegroundColor <-ConsoleColor.DarkGreen
        x|> Console.WriteLine |> ignore
        Console.ForegroundColor <- old
[<EntryPoint>]
let main argv =
    Console.ForegroundColor <-ConsoleColor.Green
    "hello for db" |> Console.WriteLine
    let conflist= getconig()
    let table_name= gettablename(conflist.[0])
    let source=(conflist.[1],table_name) |> getDbStruct |> Async.RunSynchronously |> Set.ofList
    let dest =(conflist.[2],table_name) |> getDbStruct |> Async.RunSynchronously |> Set.ofList
    printinfo "基准数据库中领先的列"
    Console.ForegroundColor <-ConsoleColor.DarkYellow
    source - dest  |> Set.map (fun x->
     x |> Console.WriteLine) |> ignore
    Console.ForegroundColor <-ConsoleColor.Red
    printinfo "基准数据库中删除的列"
    dest - source  |> Set.map (fun x->
     x |> Console.WriteLine) |> ignore
    Console.ResetColor()
    Console.Read() |> ignore
    0 // return an integer exit code
