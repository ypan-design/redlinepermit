/* Redline Permit backend adapter: gives the portal the same small API it used on Claude
   (db / assets / sample / user), backed by Supabase on redlinepermit.com. */
(function(){
  const CFG = window.RP_CONFIG || {};
  const CITY_SLUG = CFG.city || "monrovia";
  const RECOVERY = /type=recovery/.test(location.hash);
  const ready = !!(CFG.url && CFG.anonKey && window.supabase);
  const sb = ready ? window.supabase.createClient(CFG.url, CFG.anonKey, {auth:{persistSession:true, autoRefreshToken:true, detectSessionInUrl:true}}) : null;
  window.RP = {sb, ready, city:CITY_SLUG, signed:{}, recovery:RECOVERY};
  const err = (e, code) => { const x = new Error(e?.message || String(e)); x.code = code || e?.code || "error"; return x; };
  const uidNow = async () => (await sb.auth.getUser()).data.user?.id || null;

  /* ---------- clip signing: private bucket, short-lived URLs ---------- */
  async function signAll(ids){
    const need = [...new Set(ids)].filter(id => id && !RP.signed[id]);
    for(let i=0;i<need.length;i+=100){
      const {data} = await sb.storage.from("clips").createSignedUrls(need.slice(i,i+100), 60*60*12);
      (data||[]).forEach(d => { if(d.signedUrl) RP.signed[d.path] = d.signedUrl; });
    }
  }
  const assetIdsIn = obj => { const out=[]; JSON.stringify(obj, (k,v)=>{ if(k==="assetId" && typeof v==="string") out.push(v); return v; }); return out; };

  /* ---------- db ---------- */
  const MEMBER_FIELDS = r => ({role:r.role, name:r.name, email:r.email, phone:r.phone, firm:r.firm, firmRole:r.firm_role, license:r.license, title:r.title, dept:r.dept, createdAt:r.created_at, emailConfirmed:true});
  const kind = path => /\/projects$/.test(path) ? "projects" : /\/members$/.test(path) ? "members" : /\/approvals$/.test(path) ? "approvals" : null;
  const snap = rows => ({docs: rows.map(r => ({id:r.__id, exists:true, data:()=>r.__data}))});
  async function load(k){
    if(k==="projects"){
      const {data, error} = await sb.from("projects").select("id,data,updated_at").eq("city", CITY_SLUG);
      if(error) throw err(error);
      const rows = (data||[]).map(r => ({__id:r.id, __data:r.data}));
      await signAll(rows.flatMap(r => assetIdsIn(r.__data))).catch(()=>{});
      return rows;
    }
    if(k==="members"){
      const {data, error} = await sb.from("members").select("*").eq("city", CITY_SLUG);
      if(error) throw err(error);
      return (data||[]).map(r => ({__id:r.id, __data:MEMBER_FIELDS(r)}));
    }
    return [];
  }
  function collection(path){
    const k = kind(path);
    return {
      onSnapshot(cb, onErr){
        let live = true, timer = null;
        const run = () => load(k).then(rows => { if(live) cb(snap(rows)); }).catch(e => onErr && onErr(e));
        run();
        let ch = null;
        if(k==="projects" || k==="members"){
          ch = sb.channel(k+"-"+Math.random().toString(36).slice(2))
            .on("postgres_changes", {event:"*", schema:"public", table:k}, () => { clearTimeout(timer); timer = setTimeout(run, 250); })
            .subscribe();
        }
        return () => { live = false; if(ch) sb.removeChannel(ch); };
      },
      doc(id){ return docRef(k, id); }
    };
  }
  function docRef(k, id){
    return {
      async get(){
        if(k==="members"){
          const {data, error} = await sb.from("members").select("*").eq("id", id).eq("city", CITY_SLUG).maybeSingle();
          if(error) throw err(error);
          return {exists:!!data, data:()=>data?MEMBER_FIELDS(data):null};
        }
        if(k==="projects"){
          const {data, error} = await sb.from("projects").select("data").eq("id", id).maybeSingle();
          if(error) throw err(error);
          return {exists:!!data, data:()=>data?.data};
        }
        return {exists:false, data:()=>null};
      },
      async set(d){
        if(k==="projects"){
          const {error} = await sb.from("projects").upsert({id, city:CITY_SLUG, dept:d.dept||"", data:d}, {onConflict:"id"});
          if(error) throw err(error, "invalid_argument");
          return;
        }
        if(k==="members"){
          const row = {id, city:CITY_SLUG, role:d.role, name:d.name, phone:d.phone||"", firm:d.firm||"", firm_role:d.firmRole||"", license:d.license||"", title:d.title||"", dept:d.dept||""};
          const {error} = await sb.from("members").upsert(row, {onConflict:"id,city"});
          if(error) throw err(error, "invalid_argument");
          return;
        }
        throw err("Not supported", "invalid_argument");
      },
      async delete(){
        if(k==="projects"){ const {error} = await sb.from("projects").delete().eq("id", id); if(error) throw err(error); return; }
        if(k==="members"){ const {error} = await sb.from("members").delete().eq("id", id).eq("city", CITY_SLUG); if(error) throw err(error); return; }
      }
    };
  }
  const db = {
    collection,
    doc(path){ const parts = path.split("/"); const id = parts.pop(); return docRef(kind(parts.join("/")), id); }
  };

  /* ---------- assets: sheet thumbnails and clips only ---------- */
  const assets = {
    async upload(blob){
      const uid = await uidNow(); if(!uid) throw err("Sign in first","not_granted");
      const ext = (blob.type.split("/")[1]||"bin").replace("jpeg","jpg");
      const path = `${CITY_SLUG}/${uid}/${crypto.randomUUID()}.${ext}`;
      const {error} = await sb.storage.from("clips").upload(path, blob, {contentType:blob.type, upsert:false});
      if(error) throw err(error);
      return {id:path};
    }
  };

  /* ---------- AI first read (Edge Function "ai") ---------- */
  const b64 = blob => new Promise((res, rej) => { const r = new FileReader(); r.onload = () => res(String(r.result).split(",")[1]); r.onerror = rej; r.readAsDataURL(blob); });
  function parseJson(t){
    let s = String(t||"").trim().replace(/^```(?:json)?\s*/i,"").replace(/```\s*$/,"");
    try{ return JSON.parse(s); }catch(e){}
    const a = s.indexOf("{"), b = s.lastIndexOf("}"), c = s.indexOf("["), d = s.lastIndexOf("]");
    for(const [i,j] of [[a,b],[c,d]]) if(i>=0 && j>i){ try{ return JSON.parse(s.slice(i,j+1)); }catch(e){} }
    throw err("The AI answer was not valid JSON","bad_output");
  }
  async function call(prompt, opts={}, wantJson){
    const images = [];
    for(const im of (opts.images||[])) images.push({media_type: im.type || "image/jpeg", data: await b64(im)});
    const {data, error} = await sb.functions.invoke("ai", {body:{prompt, images, tier:opts.modelTier||"default", wantJson}});
    if(error){
      let body = null; try{ body = await error.context?.json?.(); }catch(e){}
      throw err(body?.error || error.message, body?.code || "ai_error");
    }
    return data;
  }
  const sample = async (prompt, opts) => { const d = await call(prompt, opts, false); return {text:d.text, truncated:!!d.truncated}; };
  sample.json = async (prompt, opts) => parseJson((await call(prompt, opts, true)).text);
  sample.limits = async () => ({images:{maxCount:12}});

  /* ---------- user ---------- */
  const user = {
    async me(){
      const u = (await sb.auth.getUser()).data.user;
      return {id:u?.id||null, name:u?.user_metadata?.profile?.name||u?.user_metadata?.full_name||u?.user_metadata?.name||"", email:u?.email_confirmed_at?u.email:null, isOwner:false, canEdit:false, avatarUrl:"", color:"#C8322B"};
    },
    async can(){ return !!(await uidNow()); },
    async isOwner(){ return false; },
    async profiles(ids){
      const out = {}; (ids||[]).forEach(id => out[id] = {id, name:""});
      const uuids = (ids||[]).filter(id => /^[0-9a-f-]{36}$/i.test(id));
      if(uuids.length){ const {data} = await sb.rpc("member_names", {ids:uuids}); (data||[]).forEach(r => out[r.id] = {id:r.id, name:r.name}); }
      return out;
    }
  };

  window.claude = {use: async name => {
    if(!sb) return null;
    if(name==="user") return user;
    const signedIn = !!(await uidNow());
    if(!signedIn) return null;
    return name==="db" ? db : name==="assets" ? assets : name==="sample" ? sample : null;
  }};
})();
