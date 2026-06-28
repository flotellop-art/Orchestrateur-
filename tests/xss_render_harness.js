
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'static', 'chat.html'), 'utf8');
const script = html.split('<script>')[1].split('</script>')[0];

function makeText(data){ return { nodeType:3, data:String(data) }; }
function makeEl(tag){
  const el = {
    nodeType:1, tagName:String(tag).toUpperCase(), childNodes:[],
    style:{}, classList:{add(){}, remove(){}},
    addEventListener(){}, remove(){},
    appendChild(c){ this.childNodes.push(c); return c; },
  };
  Object.defineProperty(el, 'textContent', {
    set(v){ this.childNodes = []; if (v !== '' && v != null) this.childNodes.push(makeText(v)); },
    get(){ return this.childNodes.map(n => n.nodeType===3 ? n.data : n.textContent).join(''); }
  });
  Object.defineProperty(el, 'className', { set(){}, get(){return '';} });
  return el;
}
const document = { createElement: makeEl, createTextNode: makeText, getElementById: ()=>makeEl('div') };
const ctx = { document, console,
  fetch: ()=>Promise.reject(new Error('no-net')),
  setTimeout, clearTimeout, Math, JSON, String,
  TextDecoder: function(){ this.decode=()=>''; }, window:{} };
vm.createContext(ctx);
vm.runInContext(script, ctx);

function walk(node, acc){
  if (!node) return;
  if (node.nodeType === 1){ acc.tags.push(node.tagName); (node.childNodes||[]).forEach(c=>walk(c,acc)); }
  else if (node.nodeType === 3){ acc.text += node.data; }
}

let failures = 0;
function check(name, cond){ console.log((cond?'PASS ':'FAIL ')+name); if(!cond) failures++; }

// 1) raw XSS payload
const payload = '<img src=x onerror=alert(1)>';
const n1 = ctx.addMessage('assistant', payload);
const a1 = {tags:[], text:''}; walk(n1, a1);
check('no IMG element created', !a1.tags.includes('IMG'));
check('no onerror attribute leaked (text only)', a1.text === payload);
check('payload preserved verbatim as text', n1.textContent === payload);

// 2) script tag payload
const p2 = '<script>alert(2)<\/script>';
const n2 = ctx.addMessage('assistant', p2);
const a2 = {tags:[], text:''}; walk(n2, a2);
check('no SCRIPT element created', !a2.tags.includes('SCRIPT'));
check('script payload kept as text', n2.textContent === p2);

// 3) markdown bold still works AND embedded html stays inert
const p3 = '**gras** <b>x</b>';
const n3 = ctx.addMessage('assistant', p3);
const a3 = {tags:[], text:''}; walk(n3, a3);
check('bold renders as STRONG element', a3.tags.includes('STRONG'));
check('no injected B element from <b>', !a3.tags.includes('B'));
check('bold text content correct', n3.textContent === 'gras <b>x</b>');

process.exit(failures === 0 ? 0 : 1);
