use chrono::{DateTime,Timelike,Utc};
use chrono_tz::Tz;
use rusqlite::{params,Connection};
use serde::{Deserialize,Serialize};
use std::{fs,sync::Mutex};
use tauri::{Manager,State};
use uuid::Uuid;

struct Db(Mutex<Connection>);
#[derive(Serialize,Deserialize)]
#[serde(rename_all="camelCase")]
struct Trade {
 id:String,account_id:String,symbol:String,asset_class:String,direction:String,timeframe:String,opened_at:String,closed_at:Option<String>,
 entry_price:f64,exit_price:Option<f64>,stop_loss:Option<f64>,take_profit:Option<f64>,quantity:f64,gross_pnl:Option<f64>,fees:f64,net_pnl:Option<f64>,
 planned_risk_usd:Option<f64>,actual_r:Option<f64>,session:String,strategy:String,note:String,status:String
}
#[derive(Deserialize)]
#[serde(rename_all="camelCase")]
struct Input {
 id:Option<String>,account_id:String,symbol:String,asset_class:String,direction:String,timeframe:String,opened_at:String,closed_at:Option<String>,
 entry_price:f64,exit_price:Option<f64>,stop_loss:Option<f64>,take_profit:Option<f64>,quantity:f64,gross_pnl:Option<f64>,fees:f64,
 planned_risk_usd:Option<f64>,strategy:String,note:String,status:String
}
#[derive(Serialize)]
#[serde(rename_all="camelCase")]
struct Point{date:String,net_pnl:f64}
#[derive(Serialize)]
#[serde(rename_all="camelCase")]
struct Session{session:String,trades:usize,wins:usize,net_pnl:f64,win_rate:Option<f64>}
#[derive(Serialize)]
#[serde(rename_all="camelCase")]
struct Overview{count:usize,wins:usize,losses:usize,net_pnl:f64,win_rate:Option<f64>,profit_factor:Option<f64>,average_r:Option<f64>,curve:Vec<Point>,sessions:Vec<Session>}
fn err(e:impl std::fmt::Display)->String{e.to_string()}
fn path(app:&tauri::AppHandle)->Result<std::path::PathBuf,String>{let d=app.path().app_data_dir().map_err(err)?;fs::create_dir_all(&d).map_err(err)?;Ok(d.join("trades.sqlite"))}
fn init(c:&Connection)->rusqlite::Result<()>{
 c.execute_batch("PRAGMA foreign_keys=ON;
 CREATE TABLE IF NOT EXISTS accounts(id TEXT PRIMARY KEY,name TEXT NOT NULL,currency TEXT NOT NULL);
 INSERT OR IGNORE INTO accounts(id,name,currency) VALUES('main','默认账户','USD');
 CREATE TABLE IF NOT EXISTS trades(
 id TEXT PRIMARY KEY,account_id TEXT NOT NULL REFERENCES accounts(id),symbol TEXT NOT NULL,asset_class TEXT NOT NULL,
 direction TEXT NOT NULL,timeframe TEXT NOT NULL,opened_at TEXT NOT NULL,closed_at TEXT,entry_price REAL NOT NULL,
 exit_price REAL,stop_loss REAL,take_profit REAL,quantity REAL NOT NULL,gross_pnl REAL,fees REAL NOT NULL,
 net_pnl REAL,planned_risk_usd REAL,actual_r REAL,session TEXT NOT NULL,strategy TEXT NOT NULL,note TEXT NOT NULL,status TEXT NOT NULL);
 CREATE INDEX IF NOT EXISTS ix_trade_date ON trades(opened_at DESC);
 CREATE TABLE IF NOT EXISTS executions(id TEXT PRIMARY KEY,trade_id TEXT NOT NULL REFERENCES trades(id) ON DELETE CASCADE,executed_at TEXT NOT NULL,side TEXT NOT NULL,price REAL NOT NULL,quantity REAL NOT NULL,fee REAL NOT NULL DEFAULT 0);
 CREATE TABLE IF NOT EXISTS trade_attachments(id TEXT PRIMARY KEY,trade_id TEXT NOT NULL REFERENCES trades(id) ON DELETE CASCADE,path TEXT NOT NULL,role TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS trade_notes(id TEXT PRIMARY KEY,trade_id TEXT NOT NULL REFERENCES trades(id) ON DELETE CASCADE,content TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS tags(id TEXT PRIMARY KEY,name TEXT NOT NULL UNIQUE);
 CREATE TABLE IF NOT EXISTS trade_tags(trade_id TEXT NOT NULL REFERENCES trades(id) ON DELETE CASCADE,tag_id TEXT NOT NULL REFERENCES tags(id),PRIMARY KEY(trade_id,tag_id));
 CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY);
 INSERT OR IGNORE INTO schema_migrations(version) VALUES(1);")
}
fn session(time:&str)->Result<String,String>{
 let dt=DateTime::parse_from_rfc3339(time).map_err(err)?.with_timezone(&Utc);
 let tz:Tz="Asia/Shanghai".parse().map_err(err)?;
 let hour=dt.with_timezone(&tz).hour();
 Ok(if hour>=8 && hour<16 {"亚盘"}else if hour>=16 && hour<21 {"欧盘"}else{"美盘"}.to_string())
}
fn valid(x:f64)->bool{x.is_finite()}
fn maybe(x:Option<f64>)->bool{x.map(valid).unwrap_or(true)}
fn validate(v:&Input)->Result<(),String>{
 if v.symbol.trim().is_empty()||v.entry_price<=0.0||v.quantity<=0.0||!valid(v.entry_price)||!valid(v.quantity)||!valid(v.fees){return Err("品种、价格、数量或费用无效".into())}
 if v.fees<0.0||![v.exit_price,v.stop_loss,v.take_profit,v.gross_pnl,v.planned_risk_usd].iter().all(|x|maybe(*x)){return Err("存在无效数值".into())}
 if !["long","short"].contains(&v.direction.as_str())||!["draft","open","closed"].contains(&v.status.as_str()){return Err("方向或状态无效".into())}
 DateTime::parse_from_rfc3339(&v.opened_at).map_err(err)?;
 if let Some(d)=&v.closed_at{DateTime::parse_from_rfc3339(d).map_err(err)?;}
 if v.status=="closed"&&(v.closed_at.is_none()||v.exit_price.is_none()||v.gross_pnl.is_none()){return Err("已平仓交易缺少出场时间、价格或毛盈亏".into())}
 Ok(())
}
fn row(r:&rusqlite::Row<'_>)->rusqlite::Result<Trade>{Ok(Trade{
 id:r.get(0)?,account_id:r.get(1)?,symbol:r.get(2)?,asset_class:r.get(3)?,direction:r.get(4)?,timeframe:r.get(5)?,opened_at:r.get(6)?,closed_at:r.get(7)?,
 entry_price:r.get(8)?,exit_price:r.get(9)?,stop_loss:r.get(10)?,take_profit:r.get(11)?,quantity:r.get(12)?,gross_pnl:r.get(13)?,fees:r.get(14)?,
 net_pnl:r.get(15)?,planned_risk_usd:r.get(16)?,actual_r:r.get(17)?,session:r.get(18)?,strategy:r.get(19)?,note:r.get(20)?,status:r.get(21)?
})}
const COLS:&str="id,account_id,symbol,asset_class,direction,timeframe,opened_at,closed_at,entry_price,exit_price,stop_loss,take_profit,quantity,gross_pnl,fees,net_pnl,planned_risk_usd,actual_r,session,strategy,note,status";
#[tauri::command]
fn list_trades(db:State<Db>)->Result<Vec<Trade>,String>{
 let c=db.0.lock().map_err(err)?;
 let mut q=c.prepare(&format!("SELECT {} FROM trades ORDER BY opened_at DESC",COLS)).map_err(err)?;
 let rows = q.query_map([], row).map_err(err)?;
 let trades = rows.collect::<rusqlite::Result<Vec<_>>>().map_err(err)?;
 Ok(trades)
}
#[tauri::command]
fn save_trade(db:State<Db>,input:Input)->Result<Trade,String>{
 validate(&input)?;
 let id=input.id.filter(|s|!s.is_empty()).unwrap_or_else(||Uuid::new_v4().to_string());
 let sess=session(&input.opened_at)?;
 let net=if input.status=="closed"{input.gross_pnl.map(|v|v-input.fees)}else{None};
 let r=match(net,input.planned_risk_usd){(Some(n),Some(r)) if r>0.0=>Some(n/r),_=>None};
 let c=db.0.lock().map_err(err)?;
 c.execute("INSERT INTO trades (id,account_id,symbol,asset_class,direction,timeframe,opened_at,closed_at,entry_price,exit_price,stop_loss,take_profit,quantity,gross_pnl,fees,net_pnl,planned_risk_usd,actual_r,session,strategy,note,status)
 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)
 ON CONFLICT(id) DO UPDATE SET symbol=excluded.symbol,asset_class=excluded.asset_class,direction=excluded.direction,timeframe=excluded.timeframe,opened_at=excluded.opened_at,closed_at=excluded.closed_at,entry_price=excluded.entry_price,exit_price=excluded.exit_price,stop_loss=excluded.stop_loss,take_profit=excluded.take_profit,quantity=excluded.quantity,gross_pnl=excluded.gross_pnl,fees=excluded.fees,net_pnl=excluded.net_pnl,planned_risk_usd=excluded.planned_risk_usd,actual_r=excluded.actual_r,session=excluded.session,strategy=excluded.strategy,note=excluded.note,status=excluded.status",
 params![id,input.account_id,input.symbol,input.asset_class,input.direction,input.timeframe,input.opened_at,input.closed_at,input.entry_price,input.exit_price,input.stop_loss,input.take_profit,input.quantity,input.gross_pnl,input.fees,net,input.planned_risk_usd,r,sess,input.strategy,input.note,input.status]).map_err(err)?;
 c.query_row(&format!("SELECT {} FROM trades WHERE id=?1",COLS),[id],row).map_err(err)
}
#[tauri::command]
fn delete_trade(db:State<Db>,id:String)->Result<(),String>{let c=db.0.lock().map_err(err)?;c.execute("DELETE FROM trades WHERE id=?1",[id]).map_err(err)?;Ok(())}
#[tauri::command]
fn get_overview(db:State<Db>)->Result<Overview,String>{
 let c=db.0.lock().map_err(err)?;
 let mut q=c.prepare("SELECT opened_at,net_pnl,actual_r,session FROM trades WHERE status='closed' AND net_pnl IS NOT NULL ORDER BY opened_at,id").map_err(err)?;
 let values=q.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,f64>(1)?,r.get::<_,Option<f64>>(2)?,r.get::<_,String>(3)?))).map_err(err)?;
 let mut count=0;let mut wins=0;let mut losses=0;let mut sum=0.0;let mut pos=0.0;let mut neg=0.0;let mut rsum=0.0;let mut rc=0;let mut curve=Vec::new();let mut groups=std::collections::BTreeMap::<String,(usize,usize,f64)>::new();
 for x in values{let (date,pnl,r,sess)=x.map_err(err)?;count+=1;sum+=pnl;if pnl>0.0{wins+=1;pos+=pnl}else if pnl<0.0{losses+=1;neg+=-pnl}if let Some(r)=r{rsum+=r;rc+=1}let v=groups.entry(sess).or_insert((0,0,0.0));v.0+=1;if pnl>0.0{v.1+=1}v.2+=pnl;curve.push(Point{date:date.chars().take(10).collect(),net_pnl:sum})}
 let sessions=groups.into_iter().map(|(session,(trades,wins,net_pnl))|Session{session,trades,wins,net_pnl,win_rate:if trades>0{Some(wins as f64/trades as f64*100.0)}else{None}}).collect();
 Ok(Overview{count,wins,losses,net_pnl:sum,win_rate:if count>0{Some(wins as f64/count as f64*100.0)}else{None},profit_factor:if neg>0.0{Some(pos/neg)}else{None},average_r:if rc>0{Some(rsum/rc as f64)}else{None},curve,sessions})
}
#[tauri::command]
fn get_storage_path(app:tauri::AppHandle)->Result<String,String>{Ok(path(&app)?.to_string_lossy().into_owned())}
#[cfg_attr(mobile,tauri::mobile_entry_point)]
pub fn run(){
 tauri::Builder::default().setup(|app|{
  let p=path(app.handle()).map_err(std::io::Error::other)?;
  let conn=Connection::open(p)?;init(&conn)?;app.manage(Db(Mutex::new(conn)));Ok(())
 }).invoke_handler(tauri::generate_handler![list_trades,save_trade,delete_trade,get_overview,get_storage_path])
 .run(tauri::generate_context!()).expect("error while running Trading Journal Pro");
}
