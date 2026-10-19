const express=require("express");
const http=require("http");
const cors=require("cors");
const {Server}=require("socket.io");
const admin=require("firebase-admin");
const app=express(), server=http.createServer(app);
const PORT=process.env.PORT||3000;
const origins=(process.env.ALLOWED_ORIGINS||"*").split(",").map(x=>x.trim());
if(process.env.FIREBASE_SERVICE_ACCOUNT){
  admin.initializeApp({
    credential:admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)),
    databaseURL:process.env.FIREBASE_DATABASE_URL
  });
}else console.warn("Firebase Admin not configured; auth/profile features will fail.");
const origin=origins.includes("*")?true:origins;
app.use(cors({origin}));
app.get("/",(_q,r)=>r.send("Pitch Side Soccer server is running."));
app.get("/health",(_q,r)=>r.json({ok:true,service:"pitch-side-soccer"}));
const io=new Server(server,{cors:{origin,methods:["GET","POST"]},pingTimeout:20000});
const rooms=new Map(), MAX=4;
const clean=(s,n=24)=>String(s||"").replace(/[<>]/g,"").trim().slice(0,n);
async function auth(socket,token){
  if(!admin.apps.length||!token)throw Error("Sign in with Google first.");
  const decoded=await admin.auth().verifyIdToken(token);socket.data.uid=decoded.uid;return decoded.uid;
}
async function profile(uid){
  const snap=await admin.database().ref("profiles/"+uid).get();
  return snap.exists()?snap.val():{name:"Player",wins:0,losses:0,matches:0};
}
function publicRoom(r){return {id:r.id,name:r.name,started:r.started,count:r.players.size,maxPlayers:MAX,players:[...r.players.values()].map(p=>({uid:p.uid,name:p.name,color:p.color}))};}
function list(){io.emit("rooms:list",[...rooms.values()].filter(r=>r.players.size).map(publicRoom));}
function update(r){io.to(r.id).emit("room:update",publicRoom(r));list();}
io.on("connection",s=>{
 s.emit("rooms:list",[...rooms.values()].filter(r=>r.players.size).map(publicRoom));
 s.on("auth",async(p={},ack=()=>{})=>{try{const uid=await auth(s,p.idToken);ack({ok:true,uid,profile:await profile(uid)});}catch(e){ack({ok:false,error:e.message});}});
 s.on("profile:get",async(_p,ack=()=>{})=>{try{if(!s.data.uid)throw Error("Sign in first.");ack({ok:true,profile:await profile(s.data.uid)});}catch(e){ack({ok:false,error:e.message});}});
 s.on("profile:name",async(p={},ack=()=>{})=>{try{if(!s.data.uid||!admin.apps.length)throw Error("Sign in and configure Firebase.");const name=clean(p.name,16)||"Player";await admin.database().ref("profiles/"+s.data.uid+"/name").set(name);ack({ok:true,name});}catch(e){ack({ok:false,error:e.message});}});
 s.on("rooms:create",async(p={},ack=()=>{})=>{try{if(!s.data.uid)throw Error("Sign in first.");const id=clean(p.roomId,32)||Math.random().toString(36).slice(2,10);if(rooms.has(id)&&rooms.get(id).players.size)throw Error("Room code is taken.");rooms.set(id,{id,name:clean(p.name,24)||"Soccer lobby",host:s.data.uid,started:false,state:null,players:new Map()});ack({ok:true,roomId:id});list();}catch(e){ack({ok:false,error:e.message});}});
 s.on("rooms:join",async(p={},ack=()=>{})=>{try{if(!s.data.uid)throw Error("Sign in first.");const id=clean(p.roomId,32),r=rooms.get(id);if(!r)throw Error("Room not found; ask the host to create it first.");if(r.started)throw Error("Match already started.");if(r.players.size>=MAX&&!r.players.has(s.data.uid))throw Error("Lobby is full.");const prof=await profile(s.data.uid);const player={uid:s.data.uid,socketId:s.id,name:clean(p.name||prof.name,16)||"Player",color:/^#[0-9a-fA-F]{6}$/.test(p.color)?p.color:"#ffd23f"};r.players.set(s.data.uid,player);s.data.roomId=id;await s.join(id);const result={ok:true,room:publicRoom(r),isHost:r.host===s.data.uid};s.emit("room:joined",result);ack(result);update(r);}catch(e){ack({ok:false,error:e.message});}});
 s.on("match:start",(ack=()=>{})=>{const r=rooms.get(s.data.roomId);if(!r)return ack({ok:false,error:"Join a lobby first."});if(r.host!==s.data.uid)return ack({ok:false,error:"Only the host can start."});if(r.players.size<2)return ack({ok:false,error:"At least 2 players must join."});r.started=true;r.state={score:[0,0],time:90,startedAt:Date.now()};io.to(r.id).emit("match:started",{room:publicRoom(r),state:r.state});update(r);ack({ok:true});});
 // Relays the host's game state; this starter does not run the physics simulation server-side yet.
 s.on("game:state",p=>{const r=rooms.get(s.data.roomId);if(!r||!r.started||r.host!==s.data.uid||!p||!Array.isArray(p.score)||p.score.length!==2)return;r.state={...p,score:p.score.map(n=>Math.max(0,Math.min(99,Number(n)||0))),time:Math.max(0,Math.min(90,Number(p.time)||0)),updatedAt:Date.now()};s.to(r.id).emit("game:state",r.state);});
 s.on("match:finish",async(p={},ack=()=>{})=>{try{const r=rooms.get(s.data.roomId);if(!r||r.host!==s.data.uid)throw Error("Only the host can finish.");const score=Array.isArray(p.score)?p.score.map(Number):[0,0];for(const [i,player] of [...r.players.values()].entries()){const ref=admin.database().ref("profiles/"+player.uid);await ref.transaction(old=>{old=old||{name:player.name,wins:0,losses:0,matches:0};old.matches=(old.matches||0)+1;if(score[0]!==score[1]){const yellow=score[0]>score[1],onYellow=i%2===0;if(yellow===onYellow)old.wins=(old.wins||0)+1;else old.losses=(old.losses||0)+1;}return old;});}io.to(r.id).emit("match:finished",{score});for(const p of r.players.values()){const peer=io.sockets.sockets.get(p.socketId);if(peer){peer.leave(r.id);delete peer.data.roomId;}}rooms.delete(r.id);list();ack({ok:true});}catch(e){ack({ok:false,error:e.message});}});
 s.on("disconnect",()=>{const id=s.data.roomId,r=rooms.get(id);if(!r)return;if(s.data.uid)r.players.delete(s.data.uid);if(r.host===s.data.uid)r.host=r.players.keys().next().value||null;if(!r.players.size)rooms.delete(id);else update(r);list();});
});
server.listen(PORT,"0.0.0.0",()=>console.log("Pitch Side Soccer listening on "+PORT));
