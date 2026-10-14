/* CKD cloud snapshot client. No patient data are sent until configuration and
   user sign-in succeed. Runs only while the portal is open. */
(function(root){
'use strict';
const SESSION_KEY='research_cloud_session_v1';
const encoder=new TextEncoder(),decoder=new TextDecoder();
function sorted(value,semantic=false){
 if(Array.isArray(value))return value.map(v=>sorted(v,semantic));
 if(value&&typeof value==='object'){
  const out={};for(const k of Object.keys(value).sort()){
   if(semantic&&['updatedAt','lastSaved'].includes(k))continue;
   out[k]=sorted(value[k],semantic);
  }return out;
 }return value;
}
async function hash(text){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',encoder.encode(text))),b=>b.toString(16).padStart(2,'0')).join('')}
function toBase64(bytes){let text='';for(let i=0;i<bytes.length;i+=8192)text+=String.fromCharCode(...bytes.subarray(i,i+8192));return btoa(text)}
function fromBase64(text){return Uint8Array.from(atob(text),x=>x.charCodeAt(0))}
async function pack(payload){
 const text=JSON.stringify(sorted(payload));const raw=encoder.encode(text);
 const semantic=structuredClone(payload);
 if(semantic.indexedDB?.meta)semantic.indexedDB.meta=semantic.indexedDB.meta.filter(x=>['reviewReasons','migrationReport'].includes(x.key)).map(x=>({key:x.key,value:x.value}));
 const digest=await hash(text),content_digest=await hash(JSON.stringify(sorted(semantic,true)));
 if(typeof CompressionStream==='function'){
  const zipped=new Uint8Array(await new Response(new Blob([raw]).stream().pipeThrough(new CompressionStream('gzip'))).arrayBuffer());
  return {digest,content_digest,encoding:'gzip-base64',payload:toBase64(zipped)};
 }return {digest,content_digest,encoding:'json-base64',payload:toBase64(raw)};
}
async function unpack(row){
 let bytes=fromBase64(row.payload);
 if(row.encoding==='gzip-base64'){
  if(typeof DecompressionStream!=='function')throw new Error('This browser cannot open compressed backups.');
  bytes=new Uint8Array(await new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'))).arrayBuffer());
 }else if(row.encoding!=='json-base64')throw new Error('Unknown backup encoding.');
 const text=decoder.decode(bytes);if(await hash(text)!==row.digest)throw new Error('Cloud backup integrity check failed.');
 const payload=JSON.parse(text);validate(payload,row);return payload;
}
function validate(payload,row){
 const data=payload?.indexedDB;
 if(payload?.backupVersion!==2||!data||!Array.isArray(data.patients)||!Array.isArray(data.visits)||!data.patients.length||!data.visits.length)throw new Error('An empty or invalid backup was blocked.');
 const ids=new Set(data.patients.map(p=>p.id)),pks=new Set();
 if(ids.size!==data.patients.length||ids.has(undefined)||ids.has(''))throw new Error('Invalid patient identities.');
 for(const v of data.visits){if(!ids.has(v.patientInternalId)||!v.pk||pks.has(v.pk))throw new Error('Invalid visit relationships.');pks.add(v.pk)}
 if(row&&(row.patient_count!==data.patients.length||row.visit_count!==data.visits.length))throw new Error('Cloud backup counts do not match.');
 return data;
}
function createClient(config,adapter){
 const allowed=/^https:\/\/[a-z0-9-]+\.supabase\.co$/;
 const url=String(config?.url||'').replace(/\/$/,'');const key=String(config?.publishableKey||'');
 let session=null,inflight=null,dirty=0,acked=0,timer=null,lastContent='',lastSaved='';
 let publicKey=key.startsWith('sb_publishable_');
 if(!publicKey&&key.split('.').length===3){try{publicKey=JSON.parse(atob(key.split('.')[1].replace(/-/g,'+').replace(/_/g,'/'))).role==='anon'}catch{}}
 const configured=allowed.test(url)&&publicKey;
 const status=(message,kind='pending')=>adapter.status(message,kind);
 function loadSession(){try{const x=JSON.parse(adapter.storage.getItem(SESSION_KEY)||'null');if(x?.project===url)session=x}catch{}}
 function storeSession(x){session={...x,project:url};adapter.storage.setItem(SESSION_KEY,JSON.stringify(session))}
 async function token(){
  if(!session?.access_token)throw new Error('Sign in to enable automatic cloud backup.');
  if(Number(session.expires_at||0)*1000<Date.now()+90000){
   const r=await adapter.fetch(url+'/auth/v1/token?grant_type=refresh_token',{method:'POST',headers:{apikey:key,'Content-Type':'application/json'},body:JSON.stringify({refresh_token:session.refresh_token})});
   if(!r.ok)throw new Error('Cloud sign-in expired. Sign in again.');const x=await r.json();storeSession({...x,expires_at:x.expires_at||Math.floor(Date.now()/1000)+x.expires_in});
  }return session.access_token;
 }
 async function request(path,options={}){
  const access=await token();const r=await adapter.fetch(url+path,{...options,headers:{apikey:key,Authorization:'Bearer '+access,'Content-Type':'application/json',...(options.headers||{})}});
  if(!r.ok){const err=new Error('Cloud request failed ('+r.status+'). Local data remain saved.');err.status=r.status;throw err}return r.status===204?null:r.json();
 }
 async function findContent(digest){const rows=await request('/rest/v1/ckd_cloud_snapshots?content_digest=eq.'+digest+'&select=*&limit=1');return rows[0]||null}
 async function latest(){const rows=await request('/rest/v1/ckd_cloud_snapshots?select=*&order=id.desc&limit=1');return rows[0]||null}
 async function send(){
  if(!configured){status('الحفظ الخارجي غير مفعّل بعد','disabled');return false}
  if(!adapter.online()){status('محفوظ على الهاتف؛ بانتظار الإنترنت','pending');return false}
  const target=dirty;status('جارٍ التحقق من النسخة الخارجية…');
  const payload=await adapter.snapshot();validate(payload);
  const row=await pack(payload);
  if(row.content_digest===lastContent){acked=target;status('آخر حفظ خارجي مؤكد: '+lastSaved,'saved');return true}
  row.patient_count=payload.indexedDB.patients.length;row.visit_count=payload.indexedDB.visits.length;
  let saved;
  try{const rows=await request('/rest/v1/ckd_cloud_snapshots',{method:'POST',headers:{Prefer:'return=representation'},body:JSON.stringify(row)});saved=rows?.[0]}
  catch(e){if(e.status!==409)throw e;saved=await findContent(row.content_digest)}
  if(!saved?.id)throw new Error('Cloud server did not confirm this backup.');
  const checkedRows=await request('/rest/v1/ckd_cloud_snapshots?id=eq.'+encodeURIComponent(saved.id)+'&select=*&limit=1');
  const checked=checkedRows[0];if(!checked||checked.content_digest!==row.content_digest)throw new Error('Cloud verification failed.');
  await unpack(checked);lastContent=row.content_digest;lastSaved=new Date(checked.created_at).toLocaleString('ar-IQ');acked=target;
  status('آخر حفظ خارجي مؤكد: '+lastSaved,'saved');return true;
 }
 function schedule(delay=10000){if(timer)return;timer=adapter.setTimeout(()=>{timer=null;flush()},delay)}
 function flush(){
  if(inflight)return inflight;
  inflight=send().catch(e=>{status(e.message,'error');return false}).finally(()=>{inflight=null;if(dirty>acked&&configured&&session?.access_token)schedule(60000)});
  return inflight;
 }
 function changed(){dirty++;status(configured?'محفوظ على الهاتف؛ النسخة الخارجية قيد الانتظار':'محفوظ على الهاتف؛ الحفظ الخارجي غير مفعّل',configured?'pending':'disabled');if(configured&&session?.access_token)schedule()}
 async function signIn(email,password){
  if(!configured)throw new Error('Cloud backup has not been configured yet.');
  const r=await adapter.fetch(url+'/auth/v1/token?grant_type=password',{method:'POST',headers:{apikey:key,'Content-Type':'application/json'},body:JSON.stringify({email,password})});
  if(!r.ok)throw new Error('تعذر تسجيل الدخول. تحققي من الحساب وكلمة المرور.');const x=await r.json();
  storeSession({...x,expires_at:x.expires_at||Math.floor(Date.now()/1000)+x.expires_in});return true;
 }
 async function signUp(email,password){
  if(!configured)throw new Error('Cloud backup has not been configured yet.');
  if(password.length<10)throw new Error('اختاري كلمة مرور من عشرة أحرف على الأقل.');
  const r=await adapter.fetch(url+'/auth/v1/signup',{method:'POST',headers:{apikey:key,'Content-Type':'application/json'},body:JSON.stringify({email,password})});
  if(!r.ok){const e=await r.json().catch(()=>({}));throw new Error(e.msg||e.message||'تعذر إنشاء الحساب. جرّبي لاحقًا.')}const x=await r.json();
  if(x.access_token){storeSession({...x,expires_at:x.expires_at||Math.floor(Date.now()/1000)+x.expires_in});return {signedIn:true}}
  return {needsConfirmation:true};
 }
 async function signInWithEmail(email){
  if(!configured)throw new Error('Cloud backup has not been configured yet.');
  const r=await adapter.fetch(url+'/auth/v1/otp',{method:'POST',headers:{apikey:key,'Content-Type':'application/json'},body:JSON.stringify({email,create_user:false})});
  if(!r.ok){const e=await r.json().catch(()=>({}));throw new Error(e.msg||e.message||'تعذر إرسال رابط الدخول. جرّبي لاحقًا.')}return true;
 }
 async function confirmEmail(email,value){
  let body;
  if(/^https:\/\//i.test(value.trim())){
   const link=new URL(value.trim());if(link.origin!==url||link.pathname!=='/auth/v1/verify')throw new Error('استخدمي رابط التفعيل الأصلي الموجود في رسالة Supabase.');
   const type=link.searchParams.get('type');if(!['signup','email','magiclink'].includes(type))throw new Error('هذا الرابط ليس لتفعيل حساب الحفظ.');
   const token_hash=link.searchParams.get('token_hash')||link.searchParams.get('token');if(!token_hash)throw new Error('رابط التفعيل غير مكتمل.');body={token_hash,type};
  }else{if(!/^\d{6,10}$/.test(value.trim()))throw new Error('الصقي رابط التفعيل أو أدخلي رمز التحقق المرسل إلى بريدك.');body={email,token:value.trim(),type:'email'}}
  const r=await adapter.fetch(url+'/auth/v1/verify',{method:'POST',headers:{apikey:key,'Content-Type':'application/json'},body:JSON.stringify(body)});
  if(!r.ok)throw new Error('لم ينجح التحقق. قد يكون الرابط مستخدمًا أو منتهي الصلاحية. إذا فعّلتِ الحساب بالفعل اختاري تسجيل الدخول.');
  const x=await r.json();if(!x.access_token)throw new Error('لم تكتمل جلسة الدخول.');storeSession({...x,expires_at:x.expires_at||Math.floor(Date.now()/1000)+x.expires_in});return true;
 }
 async function checkStartup(){
  if(!configured){status('الحفظ الخارجي غير مفعّل بعد','disabled');return {configured:false}}
  if(!adapter.online()){status('محفوظ على الهاتف؛ بانتظار الإنترنت','pending');return {configured:true,error:'Internet is needed to verify the cloud backup before opening an empty local database.'}}
  loadSession();if(!session){status('سجّلي الدخول لتفعيل الحفظ الخارجي','pending');return {configured:true,needsLogin:true}}
  // Read-only startup check. Never upload a possibly empty startup state.
  try{const row=await latest();if(!row)return {configured:true};const payload=await unpack(row);lastSaved=new Date(row.created_at).toLocaleString('ar-IQ');return {configured:true,payload,row}}
  catch(e){status(e.message,'error');return {configured:true,error:e.message}}
 }
 loadSession();return {configured,changed,flush,signIn,signUp,signInWithEmail,confirmEmail,checkStartup,latest,unpack};
}
root.CKDCloud={createClient,pack,unpack,validate};
})(typeof window!=='undefined'?window:globalThis);
