

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

let getname (conf:string) =
        conf.Split [|'='|] |>  Array.last
let makeSelect (clolist: string list,tableName:string,pkName:string)=
    let str=String.Join(",",clolist)
    sprintf "select %s from %s where %s=@%s" str tableName pkName pkName
let makeRepalce (clolist: string list,tableName:string)=
    let name= String.Join(",",clolist)
    let values= String.Join (",",clolist |> List.map(fun x->
        "@"+x))
    sprintf "REPLACE INTO %s(%s) VALUES(%s)" tableName name values

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

//同步基准库到目标库
let syncBaseToDest(baseConf:string,destConf:string,tableName:string,pkey:string,pkName:string,clolist: string list) =
    use basecon= new MySqlConnection(baseConf)
    use destcon= new MySqlConnection(destConf)
    basecon.Open()
    destcon.Open()
    use mutable basecom =new MySqlCommand()
    basecom.Connection <-basecon
    basecom.CommandText <- makeSelect(clolist,tableName,pkName)
    basecom.Parameters.AddWithValue(pkName,pkey) |> ignore
    use basereader= basecom.ExecuteReader()
    let rec read()=
        match basereader.Read() with
            | true ->
                //开始同步
                let sql= makeRepalce(clolist,tableName)
                let mutable i=0
                use mutable destcom =new MySqlCommand()
                destcom.Connection <-destcon
                destcom.CommandText <-sql
                sql |> Console.WriteLine
                clolist |> List.map(fun x->
                    //添加值到sql中
                    destcom.Parameters.AddWithValue(x,basereader.GetValue(i)) |> ignore
                    printfn "name is %s and value is %A" x (basereader.GetValue(i))
                    i<- i+1 ) |> ignore
                
                read() + 1
            | _ -> 0
    read()

[<EntryPoint>]
let main argv =
    Console.ForegroundColor <-ConsoleColor.Green
    "hello for db" |> Console.WriteLine
    let conflist= getconig()
    let table_name= getname(conflist.[0])
    let pkname= getname(conflist.[1])
    let source=(conflist.[2],table_name) |> getDbStruct |> Async.RunSynchronously |> Set.ofList
    let dest =(conflist.[3],table_name) |> getDbStruct |> Async.RunSynchronously |> Set.ofList
    printinfo "基准数据库中领先的列"
    Console.ForegroundColor <-ConsoleColor.DarkYellow
    source - dest  |> Set.map (fun x->
     x |> Console.WriteLine) |> ignore
    Console.ForegroundColor <-ConsoleColor.Red
    printinfo "基准数据库中删除的列"
    dest - source  |> Set.map (fun x->
     x |> Console.WriteLine) |> ignore
    Console.ResetColor()
    printinfo "请输入需要同步id，如果不需要同步直接回车"
    let ct= Console.ReadLine()
    let result =match ct with
                        | "" ->0
                        | x -> syncBaseToDest(conflist.[2],conflist.[3],table_name,x,pkname,Set.intersect dest source |> Set.toList)
    printfn "sync result is %d" result
    0 // return an integer exit code
