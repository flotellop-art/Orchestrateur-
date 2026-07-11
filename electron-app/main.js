const{app,BrowserWindow,Menu,Tray,shell,dialog,ipcMain}=require('electron');
const path=require('path');
const{spawn}=require('child_process');
const crypto=require('crypto');
const http=require('http');
const net=require('net');
const fs=require('fs');
const security=require('./electron_hardening');

const TITLE='Multi-Agent Orchestrator';
const INSTANCE_TOKEN=crypto.randomBytes(32).toString('base64url');
let PORT=0,BASE='';
let win=null,splash=null,tray=null,srv=null,quitting=false,relaunching=false,stopPromise=null;

function appRoot(){return path.join(__dirname,'..');}

function backendPath(){
  if(!app.isPackaged)return null;
  const name=process.platform==='win32'?'orchestrator-backend.exe':'orchestrator-backend';
  return path.join(process.resourcesPath,'backend','orchestrator-backend',name);
}

function pyPath(){
  const r=appRoot();
  for(const p of[path.join(r,'venv','Scripts','python.exe'),path.join(r,'venv','bin','python'),path.join(r,'.venv','Scripts','python.exe'),path.join(r,'.venv','bin','python')]){
    try{if(fs.existsSync(p))return p;}catch(e){}
  }
  return 'python';
}

function findFreePort(){
  return new Promise((resolve,reject)=>{
    const probe=net.createServer();
    probe.unref();
    probe.once('error',reject);
    probe.listen(0,'127.0.0.1',()=>{
      const address=probe.address();
      const port=address&&typeof address==='object'?address.port:0;
      probe.close(err=>err?reject(err):resolve(port));
    });
  });
}

function checkServer(){
  return new Promise(res=>{
    let settled=false;
    const finish=value=>{if(!settled){settled=true;res(value);}};
    const r=http.get(BASE+'/health',{headers:{'X-Orchestrator-Instance':INSTANCE_TOKEN}},resp=>{
      let body='';
      resp.setEncoding('utf8');
      resp.on('data',chunk=>{body+=chunk;if(body.length>4096)r.destroy();});
      resp.on('end',()=>{
        try{
          const data=JSON.parse(body);
          finish(resp.statusCode===200&&data.status==='ok'&&
            data.version===app.getVersion()&&data.instance===true);
        }catch(e){finish(false);}
      });
    });
    r.on('error',()=>finish(false));
    r.setTimeout(2000,()=>{r.destroy();finish(false);});
  });
}

async function startServer(){
  const root=appRoot();
  const packagedBackend=backendPath();
  const command=packagedBackend||pyPath();
  const args=packagedBackend?[]:[path.join(root,'orchestrator.py')];
  if(packagedBackend&&!fs.existsSync(packagedBackend)){
    console.error('[srv] packaged backend missing',packagedBackend);
    return false;
  }
  const dataDir=app.getPath('userData');
  const env={...process.env,ORCHESTRATOR_DATA_DIR:dataDir,PORT:String(PORT),
    ORCHESTRATOR_INSTANCE_TOKEN:INSTANCE_TOKEN};
  console.log('[srv] starting',command,args.join(' '),'data:',dataDir);
  srv=spawn(command,args,{cwd:packagedBackend?path.dirname(packagedBackend):root,env,stdio:['ignore','pipe','pipe'],windowsHide:true});
  srv.stdout.on('data',d=>console.log('[py]',d.toString().trim()));
  srv.stderr.on('data',d=>console.warn('[py!]',d.toString().trim()));
  srv.on('close',code=>{srv=null;if(!quitting)dialog.showErrorBox('Serveur arrete','Code: '+code);});
  return new Promise(res=>{
    const t=Date.now();
    const iv=setInterval(async()=>{
      if(await checkServer()){clearInterval(iv);console.log('[srv] ready');res(true);}
      else if(Date.now()-t>30000){clearInterval(iv);console.error('[srv] timeout');stopServer();res(false);}
    },500);
  });
}

function prepareBackendShutdown(){
  return new Promise(resolve=>{
    if(!BASE){resolve();return;}
    const req=http.request(BASE+'/api/runtime/prepare-shutdown',{
      method:'POST',headers:{'X-Orchestrator-Instance':INSTANCE_TOKEN}
    },resp=>{resp.resume();resp.on('end',resolve);});
    req.on('error',resolve);
    req.setTimeout(12000,()=>{req.destroy();resolve();});
    req.end();
  });
}

function stopServer(){
  if(stopPromise)return stopPromise;
  const child=srv;
  if(!child)return Promise.resolve();
  stopPromise=(async()=>{
    await prepareBackendShutdown();
    if(child.exitCode===null&&!child.killed)child.kill('SIGTERM');
    await new Promise(resolve=>{
      if(child.exitCode!==null){resolve();return;}
      const timer=setTimeout(()=>{if(child.exitCode===null)child.kill();resolve();},5000);
      child.once('close',()=>{clearTimeout(timer);resolve();});
    });
    if(srv===child)srv=null;
  })();
  return stopPromise;
}

function createSplash(){
  splash=new BrowserWindow({width:460,height:280,frame:false,alwaysOnTop:true,resizable:false,center:true,backgroundColor:'#2b2b2b',webPreferences:{nodeIntegration:false,contextIsolation:true,sandbox:true}});
  splash.loadURL('data:text/html;charset=utf-8,'+encodeURIComponent('<!DOCTYPE html><html><head><meta charset=UTF-8><style>*{margin:0;padding:0;box-sizing:border-box}body{background:#2b2b2b;color:#fff;font-family:-apple-system,sans-serif;display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;gap:20px;-webkit-app-region:drag}h1{font-size:1.5rem}h1 span{color:#E8825A}.ld{width:36px;height:36px;border:3px solid #444;border-top-color:#E8825A;border-radius:50%;animation:s .8s linear infinite}@keyframes s{to{transform:rotate(360deg)}}.st{color:#888;font-size:.8rem}</style></head><body><h1>Multi-Agent <span>Orchestrator</span></h1><div class=ld></div><p class=st>Demarrage du serveur...</p></body></html>'));
}

function buildMenu(){
  Menu.setApplicationMenu(Menu.buildFromTemplate([
    {label:TITLE,submenu:[{label:'A propos',role:'about'},{type:'separator'},{label:'Quitter',accelerator:'CmdOrCtrl+Q',click:()=>quit()}]},
    {label:'Vue',submenu:[{role:'reload',accelerator:'CmdOrCtrl+R'},{role:'zoomIn'},{role:'zoomOut'},{role:'resetZoom'},{type:'separator'},{role:'togglefullscreen',accelerator:'F11'},{type:'separator'},{role:'toggleDevTools',accelerator:'F12'}]},
    {label:'Navigation',submenu:[
      {label:'Tableau de bord',accelerator:'CmdOrCtrl+1',click:()=>win&&win.loadURL(BASE)},
      {label:'Chat',accelerator:'CmdOrCtrl+2',click:()=>win&&win.loadURL(BASE+'/chat')},
      {type:'separator'},
      {label:'Ouvrir dans navigateur',click:()=>security.safeOpenExternal(BASE)}
    ]},
    {label:'Aide',submenu:[{label:'Documentation',click:()=>security.safeOpenExternal('https://docs.anthropic.com')}]}
  ]));
}

function createWin(){
  win=new BrowserWindow({width:1280,height:800,minWidth:800,minHeight:600,show:false,title:TITLE,backgroundColor:'#f7f7f7',
    webPreferences:security.getSecureWebPreferences()});
  security.attachSecurityHooks(win);
  buildMenu();
  win.webContents.session.clearCache().then(()=>win.loadURL(BASE));
  win.once('ready-to-show',()=>{if(splash&&!splash.isDestroyed()){splash.close();splash=null;}win.show();win.focus();});
  win.on('close',e=>{if(!quitting){e.preventDefault();win.hide();}});
  win.on('closed',()=>{win=null;});
  win.webContents.on('did-fail-load',(_event,code,description,_url,isMainFrame)=>{
    if(!quitting&&isMainFrame)dialog.showErrorBox('Connexion echouee',`${description} (${code})`);
  });
}

function createTray(){
  const icon=path.join(__dirname,'assets',process.platform==='win32'?'icon.ico':'icon.png');
  try{
    tray=new Tray(icon);
    tray.setToolTip(TITLE);
    tray.setContextMenu(Menu.buildFromTemplate([
      {label:TITLE,enabled:false},{type:'separator'},
      {label:'Afficher',click:()=>show()},
      {label:'Tableau de bord',click:()=>{show();win&&win.webContents.session.clearCache().then(()=>win.loadURL(BASE));}},
      {label:'Chat',click:()=>{show();win&&win.loadURL(BASE+'/chat');}},
      {type:'separator'},{label:'Quitter',click:()=>quit()}
    ]));
    tray.on('double-click',()=>show());
  }catch(e){console.warn('[tray]',e.message);}
}

function show(){if(win){win.show();win.focus();if(win.isMinimized())win.restore();}}
async function quit(){
  if(quitting)return;
  quitting=true;
  await stopServer();
  app.quit();
}

const TRUSTED_UI_PATHS=new Set(['/','/control','/workspace','/memory','/skills','/automations','/chat']);
function isTrustedIpcEvent(event){
  try{
    const frameUrl=event.senderFrame&&event.senderFrame.url;
    const parsed=new URL(frameUrl);
    return parsed.origin===BASE&&TRUSTED_UI_PATHS.has(parsed.pathname);
  }catch(e){return false;}
}

ipcMain.handle('select-folder', async (event) => {
  if(!isTrustedIpcEvent(event))throw new Error('IPC origin refused');
  const result = await dialog.showOpenDialog(win || null, {
    properties: ['openDirectory', 'createDirectory'],
    title: 'Choisir le dossier du projet'
  });
  return result.canceled ? null : result.filePaths[0];
});

ipcMain.handle('open-external', (event, url) => {
  if(!isTrustedIpcEvent(event))throw new Error('IPC origin refused');
  security.safeOpenExternal(url);
});

ipcMain.handle('get-server-url',event=>{
  if(!isTrustedIpcEvent(event))throw new Error('IPC origin refused');
  return BASE;
});
ipcMain.handle('get-app-version',event=>{
  if(!isTrustedIpcEvent(event))throw new Error('IPC origin refused');
  return app.getVersion();
});
ipcMain.on('quit',event=>{if(isTrustedIpcEvent(event))quit();});

app.whenReady().then(async()=>{
  if(!app.requestSingleInstanceLock()){app.quit();return;}
  app.on('second-instance',()=>show());
  PORT=await findFreePort();
  BASE=`http://127.0.0.1:${PORT}`;
  security.configureLocalOrigin(BASE);
  security.setupSecureSession();
  createSplash();
  const ok=await startServer();
  if(!ok){
    if(splash&&!splash.isDestroyed())splash.close();
    const c=dialog.showMessageBoxSync({type:'error',title:'Erreur',message:'Impossible de demarrer le serveur local.',detail:'Moteur: '+(backendPath()||pyPath())+'\nDonnees: '+app.getPath('userData'),buttons:['Reessayer','Quitter']});
    if(c===0){relaunching=true;app.relaunch();}
    app.quit();return;
  }
  createWin();
  createTray();
});

app.on('activate',()=>{if(BrowserWindow.getAllWindows().length===0)createWin();else show();});
app.on('before-quit',event=>{
  if(!relaunching&&srv){
    event.preventDefault();
    if(!quitting)quitting=true;
    stopServer().finally(()=>app.quit());
  }else{
    quitting=true;
  }
});
process.on('SIGTERM',()=>quit());
process.on('SIGINT',()=>quit());
