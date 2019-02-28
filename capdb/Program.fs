

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
    let str=String.Join(",",clolist |> List.map(fun x->
        "`"+x+"`"))
    sprintf "select %s from %s where %s=@%s" str tableName pkName pkName
let makeRepalce (clolist: string list,tableName:string)=
    let name= String.Join(",",clolist |> List.map(fun x->
        "`"+x+"`"))
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
type myPrint=
    static member printinfo (x:string,?infocolor:ConsoleColor) =
            let col=defaultArg infocolor ConsoleColor.DarkGreen
            let old= Console.ForegroundColor
            Console.ForegroundColor <-col
            x|> Console.WriteLine |> ignore
            Console.ForegroundColor <- old

//查询目标库值，如果只有一列说明主键唯一，则执行diff比较
let getDestVaule(destcon:MySqlConnection,tableName:string,pkey:string,pkName:string,clolist: string list)=
    use mutable destcom =new MySqlCommand()
    destcom.Connection<-destcon
    destcom.CommandText <- makeSelect(clolist,tableName,pkName)
    destcom.Parameters.AddWithValue(pkName,pkey) |> ignore
    use destreader= destcom.ExecuteReader()
    let mutable one=true
    let rec read(result:option<Map<string,obj>>)=
            match destreader.Read() with
            | true -> 
                match one with
                    | true->
                        let mutable i=0
                        let res=Some(clolist |> List.map(fun x ->
                            i<-i+1
                            (x,destreader.GetValue(i-1))
                        )|> Map.ofList)
                        one<-false
                        read(res)
                    | _ -> None
            | _ -> result
    read(None)
        
 
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
    let destmap=getDestVaule(destcon,tableName,pkey,pkName,clolist)
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
                    match destmap with
                    |Some(dmap) ->
                        let old=basereader.GetValue(i)
                        if Convert.ToString(old)=Convert.ToString(dmap.[x]) then
                            printfn "name is %s and value is %A" x (basereader.GetValue(i))
                        else
                            myPrint.printinfo(sprintf "name is %s and value is %A -> %A" x dmap.[x] (basereader.GetValue(i)),ConsoleColor.Blue)
                    |None->
                        printfn "name is %s and value is %A" x (basereader.GetValue(i))
                    i<- i+1 ) |> ignore
                let rownum=destcom.ExecuteNonQuery()
                printfn "replace %d row " rownum
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
    myPrint.printinfo "基准数据库中领先的列"
    Console.ForegroundColor <-ConsoleColor.DarkYellow
    source - dest  |> Set.map (fun x->
     x |> Console.WriteLine) |> ignore
    Console.ForegroundColor <-ConsoleColor.Red
    myPrint.printinfo "基准数据库中删除的列"
    dest - source  |> Set.map (fun x->
     x |> Console.WriteLine) |> ignore
    Console.ResetColor()
    myPrint.printinfo "请输入需要同步id，如果不需要同步直接回车"
    let ct= Console.ReadLine()
    let result =match ct with
                        | "" ->0
                        | x -> syncBaseToDest(conflist.[2],conflist.[3],table_name,x,pkname,Set.intersect dest source |> Set.toList)
    printfn "sync result is %d" result
    0 // return an integer exit code
