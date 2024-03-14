use actix_web::{post, web, App, HttpServer, Responder, Result};
use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::path::PathBuf;
use std::io::{prelude::*, BufReader};
use tiny_keccak::{Hasher, Sha3};
use serde::{Deserialize, Serialize}; 
use substring::Substring;
use rocksdb::{DB, Options};
use wasmtime::{Extern, Caller, Instance, Memory, MemoryType, SharedMemory, Config, Engine, Linker, Module, Store, Mutability, GlobalType, Global, Val, ValType};
use rlp::{Rlp};

#[derive(Deserialize)]
struct JsonrpcRequest {
    id: u32,
    method: String,
    params: Vec<String>,
    jsonrpc: String,
}
#[derive(Serialize)]
struct  JsonrpcError{
    id: u32,
    error: String,
    jsonrpc: String,
}
#[derive(Serialize)]
struct JsonrpcResult{
    id: u32, 
    result: String,
    jsonrpc: String,
}

struct Context {
    hash: [u8; 32],
    program: Vec<u8>,
}

pub fn db_annotate_value(v: &Vec<u8>, block_height: u32) -> Vec<u8> {
    let mut entry: Vec<u8> = v.clone();
    let height: Vec<u8> = block_height.to_le_bytes().try_into().unwrap();
    entry.extend(height);
    return entry;
  }
  
  pub fn db_make_list_key(v: &Vec<u8>, index: u32) -> Vec<u8> {
    let mut entry = v.clone();
    let index_bits: Vec<u8> = index.to_le_bytes().try_into().unwrap();
    entry.extend(index_bits);
    return entry;
  }
  
  pub fn db_make_length_key(key: &Vec<u8>) -> Vec<u8> {
    return db_make_list_key(key, u32::MAX);
  }
  
  pub fn db_append(db: &'static DB, batch: &mut rocksdb::WriteBatch, key: &Vec<u8> , value: &Vec<u8>) {
    let mut length_key = db_make_length_key(key);
    let length: u32 = db_length_at_key(db
, &length_key);
    let entry_key: Vec<u8> = db_make_list_key(key, length);
    batch.put(&entry_key, &value);
    let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
    batch.put(&length_key, &new_length_bits);
  }
  
  
  pub fn db_length_at_key(db: &'static DB, length_key: &Vec<u8>) -> u32 {
    return match db.get(length_key).unwrap() {
      Some(v) => u32::from_le_bytes(v.try_into().unwrap()),
      None => 0
    }
  }
  
  pub fn read_arraybuffer_as_vec(data: &[u8], data_start: i32) -> Vec<u8> {
        let len = u32::from_le_bytes((data[((data_start - 4) as usize)..(data_start as usize)]).try_into().unwrap());
        return Vec::<u8>::from(&data[(data_start as usize)..(((data_start as u32) + len) as usize)]);
  }
  
    
  
  pub fn setup_linker(linker: &mut Linker<()>, store: &mut Store<()>, db: &'static DB, input: &Vec<u8>, height: u32) {
      let mut input_clone: Vec<u8> = <Vec<u8> as TryFrom<[u8; 4]>>::try_from(height.to_le_bytes()).unwrap();
      input_clone.extend(input.clone());
      let __host_len = input_clone.len();
      linker.func_wrap("env", "__host_len", move |mut caller: Caller<'_, ()>| -> i32 {
        return __host_len.try_into().unwrap();
      }).unwrap();
      linker.func_wrap("env", "__load_input", move |mut caller: Caller<'_, ()>, data_start: i32| {
        let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
        let _ = mem.write(&mut caller, data_start.try_into().unwrap(), input_clone.as_slice());
      }).unwrap();
      linker.func_wrap("env", "__log", |mut caller: Caller<'_, ()>, data_start: i32| {
        let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
        let data = mem.data(&caller);
        let bytes = read_arraybuffer_as_vec(data, data_start);
        println!("{}", std::str::from_utf8(bytes.as_slice()).unwrap());
      }).unwrap();
      linker.func_wrap("env", "abort", |_: i32, _: i32, _: i32, _: i32| {
        panic!("abort!");
      }).unwrap();
  }
  
  
  pub fn db_append_annotated(db: &'static DB, batch: &mut rocksdb::WriteBatch, key: &Vec<u8> , value: &Vec<u8>, block_height: u32) {
    let mut length_key = db_make_length_key(key);
    let length: u32 = db_length_at_key(db, &length_key);
    let entry = db_annotate_value(value, block_height);
  
    let entry_key: Vec<u8> = db_make_list_key(key, length);
    batch.put(&entry_key, &entry);
    let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
    batch.put(&length_key, &new_length_bits);
  }
  
  pub fn db_create_empty_update_list(batch: &mut rocksdb::WriteBatch, height: u32){
      let height_vec: Vec<u8> = height.to_le_bytes().try_into().unwrap();
      let key: Vec<u8> = db_make_length_key(&db_make_updated_key(&height_vec));
      let value_vec: Vec<u8> = (0 as u32).to_le_bytes().try_into().unwrap();
      batch.put(&key, &value_vec);
  }
  pub fn setup_linker_indexer(linker: &mut Linker<()>, db: &'static DB, height: usize) {
      linker.func_wrap("env", "__flush", move |mut caller: Caller<'_, ()>, encoded: i32| {
        let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
        let data = mem.data(&caller);
        let encoded_vec = read_arraybuffer_as_vec(data, encoded);
        let mut batch = rocksdb::WriteBatch::default();
        let _ = db_create_empty_update_list(&mut batch, height as u32);
        let decoded: Vec<Vec<u8>> = rlp::decode_list(&encoded_vec);
        decoded.iter().tuple_windows().inspect(|(k, v)| {
          let k_owned = <Vec<u8> as Clone>::clone(k);
          let v_owned = <Vec<u8> as Clone>::clone(v);
          db_append_annotated(db, &mut batch, &k_owned, &v_owned, height as u32);
          let update_key: Vec<u8> = <Vec<u8> as TryFrom<[u8; 4]>>::try_from((height as u32).to_le_bytes()).unwrap();
          db_append(db, &mut batch, &update_key, &k_owned);
        });
        println!("saving {:?} k/v pairs for block {:?}", decoded.len() / 2, height);
        (db).write(batch).unwrap();
      }).unwrap();
      linker.func_wrap("env", "__get", move |mut caller: Caller<'_, ()>, key: i32, value: i32| {
        let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
        let data = mem.data(&caller);
        let key_vec = read_arraybuffer_as_vec(data, key);
        let length = db_length_at_key(db, &key_vec);
        if length != 0 {
          let indexed_key = db_make_list_key(&key_vec, length - 1);
          let mut value_vec = (db).get(&indexed_key).unwrap().unwrap();
          value_vec.truncate(value_vec.len().saturating_sub(4));
          let _ = mem.write(&mut caller, value.try_into().unwrap(), value_vec.as_slice());
        }
      }).unwrap();
      linker.func_wrap("env", "__get_len", move |mut caller: Caller<'_, ()>, key: i32| -> i32 {
        let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
        let data = mem.data(&caller);
        let key_vec = read_arraybuffer_as_vec(data, key);
        let length = db_length_at_key(db, &key_vec);
        if length != 0 {
          let indexed_key = db_make_list_key(&key_vec, length - 1);
          let value_vec = (db).get(&indexed_key).unwrap().unwrap();
          return (value_vec.len() - 4).try_into().unwrap();
        } else {
          return 0;
        }
      }).unwrap();
  }
  
  pub fn db_set_length(db: &'static DB, key: &Vec<u8>, length: u32) {
    let mut length_key = db_make_length_key(key);
    if length == 0 {
      db.delete(&length_key).unwrap();
      return;
    }
    let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
    db.put(&length_key, &new_length_bits).unwrap();
  }
  
  pub fn db_updated_keys_for_block(db: &'static DB, height: u32) -> HashSet<Vec<u8>> {
    let key: Vec<u8> = db_make_length_key(&db_make_updated_key(&u32_to_vec(height)));
    let length: i32 = (db_length_at_key(db
, &key) as i32);
    let mut i: i32 = 0;
    let mut set: HashSet<Vec<u8>> = HashSet::<Vec<u8>>::new();
    while i < length {
      set.insert(db.get(&db_make_list_key(&key, i as u32)).unwrap().unwrap());
      i = i + 1;
    }
    return set;
  }
  
  pub fn db_updated_keys_for_block_range(db: &'static DB, from: u32, to: u32) -> HashSet<Vec<u8>> {
    let mut i = from;
    let mut result: HashSet<Vec<u8>> = HashSet::<Vec<u8>>::new();
    while to >= i {
      result.extend(db_updated_keys_for_block(db
, i));
      i = i + 1;
    }
    return result;
  }
  
  
  pub fn db_rollback_key(db: &'static DB, key: &Vec<u8>, to_block: u32) {
    let length: i32 = db_length_at_key(db
, &key).try_into().unwrap();
    let mut index: i32 = length - 1;
    let mut end_length: i32 = length;
    while index >= 0 {
      let list_key = db_make_list_key(key, index.try_into().unwrap());
      let _ = match db.get(&list_key).unwrap() {
        Some(value) => {
          let value_height: u32 = u32::from_le_bytes(value.as_slice()[(value.len() - 4)..].try_into().unwrap());
          if to_block <= value_height.try_into().unwrap() {
            db.delete(&list_key).unwrap();
            end_length = end_length - 1;
          } else {
            break;
          }
        },
        None => { break; }
      };
    }
    if end_length != length {
      db_set_length(db
, key, end_length as u32);
    }
  }
  
  pub fn db_value_at_block(db: &'static DB, key: &Vec<u8>, height: i32) -> Vec<u8> {
    let length: i32 = db_length_at_key(db
, &key).try_into().unwrap();
    let mut index: i32 = length - 1;
    while index >= 0 {
      let value: Vec<u8> = match db.get(db_make_list_key(key, index.try_into().unwrap())).unwrap() {
        Some(v) => v,
        None => db_make_list_key(&Vec::<u8>::new(), 0)
      };
  
      let value_height: u32 = u32::from_le_bytes(value.as_slice()[(value.len() - 4)..].try_into().unwrap());
      /*
        Ok(v) => u32::from_le_bytes(v).try_into().unwrap(),
        Err(e) => 0
      };
      */
      if height >= value_height.try_into().unwrap() {
        value.clone().truncate(value.len().saturating_sub(4));
      }
    }
    return vec![];
  }
  
  pub fn setup_linker_view(linker: &mut Linker<()>, db: &'static DB, height: i32) {
      linker.func_wrap("env", "__flush", move |mut caller: Caller<'_, ()>, encoded: i32| {}).unwrap();
      linker.func_wrap("env", "__get", move |mut caller: Caller<'_, ()>, key: i32, value: i32| {
        let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
        let data = mem.data(&caller);
        let key_vec = read_arraybuffer_as_vec(data, key);
        let value = db_value_at_block(db
    , &key_vec, height);
        let _ = mem.write(&mut caller, value.len(), value.as_slice());
      }).unwrap();
      linker.func_wrap("env", "__get_len", move |mut caller: Caller<'_, ()>, key: i32| -> i32 {
        let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
        let data = mem.data(&caller);
        let key_vec = read_arraybuffer_as_vec(data, key);
        let value = db_value_at_block(db
    , &key_vec, height);
        return value.len().try_into().unwrap();
      }).unwrap();
  }
  
  pub fn u32_to_vec(v: u32) -> Vec<u8> {
    return v.to_le_bytes().try_into().unwrap();
  }
  
  pub fn check_latest_block_for_reorg(db: &'static DB, height: u32) -> u32 {
      match db.get(db_make_length_key(&db_make_updated_key(&u32_to_vec(height as u32)))).unwrap() {
          Some(v) => check_latest_block_for_reorg(db
    , height + 1),
          None => return height
      }
  }
  pub fn db_make_updated_key(key: &Vec<u8>)-> Vec<u8>{
      return key.clone();
  }

#[post("/")]
async fn view(body: web::Json<JsonrpcRequest>, context: web::Data<Context>) -> Result<impl Responder>{
    if body.method != "metashrew_view" {
        let resp = JsonrpcError {id: body.id, error: "Unsupported method".to_string(), jsonrpc: "2.0".to_string() };
        return Ok(web::Json(resp));
    } else {
        if hex::decode(body.params[0].to_string().substring(2, (body.params[0].len() - 2))).unwrap() != context.hash {
            let resp = JsonrpcError {id: body.id, error: "Hash doesn't match".to_string(), jsonrpc: "2.0".to_string() };
            return Ok(web::Json(resp));
        } 
        let db_path = match env::var("DB_LOCATION"){
            Ok(val) => val,
            Err(e) => "/mnt/volume/rocksdb".to_string(),
        };
        let db = DB::open_for_read_only(&Options::default(), db_path, false).unwrap();
        let engine = wasmtime::Engine::default();
        let module = wasmtime::Module::from_binary(&engine, context.program.as_slice()).unwrap();
        let mut store = Store::new(&engine, ());
        let mut linker = Linker::new(&engine);
        let block_tag = body.params[3];
        let height: i32 = match block_tag.parse() {
          Ok(v) => v,
          Err(e) => if block_tag == "latest" { 100 //FIX THIS
             } else { -1 }
        };
        // if height < 0 {
        //   return Ok(json!({ "error": format!("invalid block_tag: {:?}", block_tag) }));
        // }
        let input_rlp = body.params[2];
        let input: Vec<u8> = input_rlp.as_str().try_into().unwrap();
        setup_linker(&mut linker, &mut store, &db, &input, height as u32);
        setup_linker_view(&mut linker, &db, height);
        let instance = linker.instantiate(&mut store, &module).unwrap();
        instance.get_memory(&mut store, "memory").unwrap().grow(&mut store,  128).unwrap();
        let symbol = body.params[2];
        let fnc = instance.get_typed_func::<(), (i32)>(&mut store, symbol.as_str()).unwrap();
        let result = fnc.call(&mut store, ()).unwrap();
        let mem = instance.get_memory(&mut store, "memory").unwrap();
        let data = mem.data(&mut store);
        let encoded_vec = read_arraybuffer_as_vec(data, result);
        return Ok(web::Json(JsonrpcResult{id: body.id, result: hex::encode(encoded_vec), jsonrpc: "2.0".to_string()}));
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()>{
    // get the 
    let path = match env::var("PROGRAM_PATH"){
        Ok(val) => val,
        Err(e) => PathBuf::from("/mnt/volume/indexer.wasm").to_str().unwrap().try_into().unwrap(),
    };
    let program = File::open(path).expect("msg");
    let mut buf = BufReader::new(program);
    let mut bytes: Vec<u8> = vec![];
    buf.read_to_end(&mut bytes);
    let hasher = Sha3::v256();
    let mut output = [0; 32];
    hasher.update(bytes.as_slice());
    hasher.finalize(&mut output);

    HttpServer::new(|| {
        App::new().app_data(web::Data::new(Context {hash: output, program: bytes})).service(view)
    })
    .bind((
        match env::var("HOST"){
            Ok(val)=> val.as_str(),
            Err(e) => "127.0.0.1", },
        match env::var("PORT") {
            Ok(val) => val.parse::<u16>().unwrap(),
            Err(e) => 8080,
        }))
}

