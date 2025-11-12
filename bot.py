import os
import asyncio
import json
import time
import logging
from typing import Dict, Any, List, Tuple, Callable
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters

# Global variables for chat logging
_DEBUG_CHAT_ID = None
_BOT_INSTANCE = None

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO
)

# Custom logging handler for sending logs to chat
class ChatLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.setLevel(logging.DEBUG)
        
    def emit(self, record):
        if _DEBUG_CHAT_ID and _BOT_INSTANCE:
            try:
                log_message = self.format(record)
                # Truncate very long messages
                if len(log_message) > 4000:
                    log_message = log_message[:4000] + "..."
                
                # Send log message to chat asynchronously
                asyncio.create_task(
                    _BOT_INSTANCE.send_message(
                        chat_id=_DEBUG_CHAT_ID, 
                        text=f"🐛 <code>{log_message}</code>", 
                        parse_mode='HTML'
                    )
                )
            except Exception:
                pass  # Ignore errors in logging to avoid recursion

# Create chat log handler
_chat_log_handler = ChatLogHandler()


def clean_btn(text: str) -> str:
    if not text:
        return ''
    t = str(text)
    import re
    t = re.sub(r'^[^A-Za-zА-Яа-я0-9]+', '', t)
    t = re.sub(r'[\u00A0\u1680\u2000-\u200A\u202F\u205F\u3000\u200B\s]+', ' ', t)
    t = re.sub(r'[\.,:;!\-–—_/]', '', t)
    t = t.strip().lower()
    return t


def reply_kb() -> ReplyKeyboardMarkup:
    keyboard = [
        ['🔎 Поиск препарата по названию', '🧪 Поиск по д.в.'],
        ['📋 Подбор пестицида', '🧮 Калькулятор расхода препарата'],
        ['ℹ️ Помощь', '📞 Контакты']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False, input_field_placeholder='Выберите действие или введите название...')


_COL_ALIASES = {
    'type': ['Вид препарата (пестицид)','Вид препарата (пестицида)','Вид препарата','Тип','Тип препарата','Тип (пестицид)'],
    'destroy': ['Вид уничтожаемого объекта','Вид уничтож. объекта','Вид уничтожаемого об'],
    'name': ['Название препарата','Препарат','Наименование'],
    'ai': ['Действующее вещество','Д.в.','Активное вещество'],
    'crops': ['Культуры','Культура'],
    'pests': ['Вредные объекты','Вредители','Тип вредителя'],
    'rate': ['Норма расхода','Норма применения','Расход']
}

_DATA_CACHE: Dict[str, Any] = {'data': None, 'expires': 0}
_CONTACTS_CACHE: Dict[str, Any] = {'data': None, 'expires': 0}
_CACHE_TTL = 3600


def _norm_header(s: str) -> str:
    return str(s or '').lower().replace('\t','').replace('\n','').replace('\r','').replace(' ', '').replace('-', '').replace('_','').replace('.', '').replace(':','').replace('(', '').replace(')', '').replace('ё', 'е')


def get_val(row: Dict[str, Any], kind: str) -> str:
    aliases = _COL_ALIASES.get(kind, [])
    for h in aliases:
        if h in row and str(row[h]).strip() != '':
            return str(row[h])
    keys = { _norm_header(k): k for k in row.keys() }
    for a in aliases:
        nk = _norm_header(a)
        if nk in keys and str(row[keys[nk]]).strip() != '':
            return str(row[keys[nk]])
    return ''


def normalize_text(s: str) -> str:
    import re
    return re.sub(r'\s+', ' ', re.sub(r'[^a-zа-я0-9\s]', ' ', str(s or '').lower().replace('ё','е'))).strip()


_RUS_TO_EN = {'й':'q','ц':'w','у':'e','к':'r','е':'t','н':'y','г':'u','ш':'i','щ':'o','з':'p','х':'[','ъ':']','ф':'a','ы':'s','в':'d','а':'f','п':'g','р':'h','о':'j','л':'k','д':'l','ж':';','э':'\'','я':'z','ч':'x','с':'c','м':'v','и':'b','т':'n','ь':'m','б':',','ю':'.'}
_EN_TO_RUS = {v: k for k, v in _RUS_TO_EN.items()}


def switch_layout(s: str) -> Tuple[str, str]:
    low = str(s or '').lower()
    to_en = ''.join([_RUS_TO_EN.get(ch, ch) for ch in low])
    to_ru = ''.join([_EN_TO_RUS.get(ch, ch) for ch in low])
    return to_en, to_ru


def translit_simple(s: str) -> str:
    low = str(s or '').lower()
    low = low.replace('sch','щ').replace('sh','ш').replace('zh','ж').replace('ch','ч').replace('yo','ё').replace('yu','ю').replace('ya','я')
    m = {'e':'е','a':'а','o':'о','i':'и','u':'у','k':'к','h':'х','g':'г','t':'т','r':'р','s':'с','d':'д','l':'л','m':'м','p':'п','b':'б','v':'в','f':'ф','y':'ы'}
    return ''.join([m.get(c, c) for c in low])


def _lev(a: str, b: str) -> int:
    a = a or ''
    b = b or ''
    m = len(a)
    n = len(b)
    if not m:
        return n
    if not n:
        return m
    dp = list(range(n+1))
    for i in range(1, m+1):
        prev = i-1
        dp[0] = i
        for j in range(1, n+1):
            tmp = dp[j]
            cost = 0 if a[i-1] == b[j-1] else 1
            dp[j] = min(dp[j]+1, dp[j-1]+1, prev+cost)
            prev = tmp
    return dp[n]


def fuzzy_score(a: str, b: str) -> float:
    if not a or not b:
        return 1e9
    d = _lev(a, b)
    return d / max(len(a), len(b))


def _fill_down(headers: List[str], rows: List[Dict[str, Any]]):
    for h in headers:
        last = ''
        for r in rows:
            val = str(r.get(h, '') or '').strip()
            if val != '':
                last = str(r.get(h, ''))
                r[h] = last
            else:
                r[h] = last


def _open_sheet(sheet_id: str, sheet_name: str = None):
    try:
        import gspread
        from google.oauth2 import service_account
        creds_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
        credentials = None
        if creds_json:
            info = json.loads(creds_json)
            credentials = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
        else:
            cred_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if cred_path and os.path.exists(cred_path):
                credentials = service_account.Credentials.from_service_account_file(cred_path, scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
        if not credentials:
            return None
        gc = gspread.authorize(credentials)
        ss = gc.open_by_key(sheet_id)
        if sheet_name:
            sh = ss.worksheet(sheet_name)
        else:
            sh = ss.sheet1
        return sh
    except Exception:
        return None


async def ensure_data_loaded(force: bool = False) -> Dict[str, Any]:
    now = time.time()
    if not force and _DATA_CACHE['data'] is not None and _DATA_CACHE['expires'] > now:
        return _DATA_CACHE['data']
    sheet_id = os.getenv('SHEET_ID')
    sheet_name = os.getenv('SHEET_NAME')
    if not sheet_id:
        _DATA_CACHE['data'] = {'headers': [], 'rows': []}
        _DATA_CACHE['expires'] = now + _CACHE_TTL
        return _DATA_CACHE['data']
    sh = _open_sheet(sheet_id, sheet_name)
    values = sh.get_all_values() if sh else []
    if not values:
        data = {'headers': [], 'rows': []}
        _DATA_CACHE['data'] = data
        _DATA_CACHE['expires'] = now + _CACHE_TTL
        return data
    header_row = 0
    for i, row in enumerate(values):
        non_empty = len([x for x in row if str(x).strip() != ''])
        if non_empty >= 3:
            header_row = i
            break
    headers = [str(h).strip() for h in values[header_row]]
    rows: List[Dict[str, Any]] = []
    for r in values[header_row+1:]:
        obj = {}
        for c, h in enumerate(headers):
            obj[h] = str(r[c] if c < len(r) else '')
        rows.append(obj)
    _fill_down(headers, rows)
    for r in rows:
        ai_val = get_val(r, 'ai')
        ai_norm = normalize_text(ai_val)
        ai_words = [w for w in ai_norm.split(' ') if len(w) >= 3]
        r['_aiWords'] = ai_words
    data = {'headers': headers, 'rows': rows}
    _DATA_CACHE['data'] = data
    _DATA_CACHE['expires'] = now + _CACHE_TTL
    return data


async def ensure_contacts_loaded(force: bool = False) -> List[Dict[str, Any]]:
    now = time.time()
    if not force and _CONTACTS_CACHE['data'] is not None and _CONTACTS_CACHE['expires'] > now:
        return _CONTACTS_CACHE['data']
    sheet_id = os.getenv('SHEET_ID')
    if not sheet_id:
        _CONTACTS_CACHE['data'] = []
        _CONTACTS_CACHE['expires'] = now + _CACHE_TTL
        return []
    sh = _open_sheet(sheet_id, 'Контакты')
    values = sh.get_all_values() if sh else []
    if len(values) < 2:
        _CONTACTS_CACHE['data'] = []
        _CONTACTS_CACHE['expires'] = now + _CACHE_TTL
        return []
    headers = [str(h).strip() for h in values[0]]
    contacts: List[Dict[str, Any]] = []
    for r in values[1:]:
        obj = {}
        for c, h in enumerate(headers):
            obj[h] = str(r[c] if c < len(r) else '').strip()
        if obj.get('Филиал/Офис'):
            contacts.append(obj)
    _CONTACTS_CACHE['data'] = contacts
    _CONTACTS_CACHE['expires'] = now + _CACHE_TTL
    return contacts


def kb(rows: List[List[Dict[str, str]]]) -> Dict[str, Any]:
    return { 'inline_keyboard': rows }


def hash32(s: str) -> str:
    s = str(s or '')
    h = 0
    for ch in s:
        h = ((h << 5) - h) + ord(ch)
        h &= 0xFFFFFFFF
    return format(h & 0xFFFFFFFF, 'x')


def crop_key_for_dedup(s: str) -> str:
    t = str(s or '').lower()
    import re
    t = t.replace('ё','е')
    t = re.sub(r'[\s\u00A0\u2007\u202F]+',' ',t).strip()
    t = re.sub(r'\bяров(ой|ая|ые)\b','яров*',t)
    t = re.sub(r'\bозим(ый|ая|ые)\b','озим*',t)
    t = re.sub(r'\bл[еe]н(\s+масличный)?\b','лен*',t)
    return t


def unify_season_ending(s: str) -> str:
    return str(s or '').replace('яровая','яровая').replace('яровые','яровая').replace('яровой','яровая').replace('озимая','озимая').replace('озимые','озимая').replace('озимый','озимая')


def normalize_crop_name(s: str) -> str:
    s = str(s or '').strip()
    if not s:
        return ''
    t = s[0].upper() + s[1:].strip() if len(s)>1 else s.upper()
    return unify_season_ending(t)


def split_crops_field(s: str) -> List[str]:
    import re
    base = str(s or '').replace('\u00A0',' ').replace('\u2007',' ').replace('\u202F',' ').split(',')
    tmp = []
    for item in base:
        tmp.extend([x.strip() for x in item.split(';')])
    base = [x for x in tmp if x]
    result = []
    for item in base:
        m1 = __import__('re').match(r'^(.+?)\s+и\s+(.+?)\s+яровые$', item, flags=re.I)
        if m1:
            result.append(normalize_crop_name(m1.group(1) + ' яровая'))
            result.append(normalize_crop_name(m1.group(2) + ' яровая'))
            continue
        m1b = re.match(r'^(.+?)\s+и\s+(.+?)\s+озимые$', item, flags=re.I)
        if m1b:
            result.append(normalize_crop_name(m1b.group(1) + ' озимая'))
            result.append(normalize_crop_name(m1b.group(2) + ' озимая'))
            continue
        m2 = re.match(r'^(.+?)\s+яровая\s+и\s+озимая$', item, flags=re.I)
        if m2:
            result.append(normalize_crop_name(m2.group(1) + ' яровая'))
            result.append(normalize_crop_name(m2.group(1) + ' озимая'))
            continue
        parts = [p.strip() for p in re.split(r'\s+и\s+', item) if p.strip()]
        if len(parts)>1 and not __import__('re').search(r'[аеиоуыэюя]$', parts[-1], flags=re.I):
            for p in parts:
                result.append(normalize_crop_name(p))
            continue
        result.append(normalize_crop_name(item))
    seen = {}
    out = []
    for c in result:
        k = normalize_text(unify_season_ending(c))
        if not seen.get(k):
            seen[k] = True
            out.append(unify_season_ending(c))
    return out


def title_case(s: str) -> str:
    t = str(s or '').lower()
    out = []
    i = 0
    while i < len(t):
        ch = t[i]
        out.append(ch.upper() if i == 0 or t[i-1] in ' -(\n' else ch)
        i += 1
    return ''.join(out)


def pretty_crop_label(s: str) -> str:
    s = str(s or '').strip()
    parts = s.split()
    if not parts:
        return s
    first = parts[0]
    rest = ' '.join(parts[1:])
    masculine = True if len(first)>0 and first[-1].lower() in ['ь','й'] else False
    rest = rest.lower().replace('яровой','яровой' if masculine else 'яровая').replace('озимый','озимый' if masculine else 'озимая')
    lbl = first[:1].upper()+first[1:].lower()
    if rest:
        lbl += ' ' + rest
    return lbl


def short_type_label(t: str) -> str:
    t = str(t or '').strip()
    import re
    m = re.match(r'^([^\s\-(]+)', t)
    return m.group(1) if m else (t or 'Вид')


def unique_destroy_kinds_for_crop(rows: List[Dict[str, Any]], crop: str) -> List[str]:
    s = {}
    ck = crop_key_for_dedup(crop)
    for r in rows:
        cropCol = get_val(r,'crops')
        kindsCol = get_val(r,'destroy')
        if not kindsCol:
            continue
        options = [crop_key_for_dedup(x) for x in split_crops_field(str(cropCol))]
        if ck not in options:
            continue
        for k in [x.strip() for x in str(kindsCol).split(',') if x.strip()]:
            s[title_case(k)] = True
    return sorted(list(s.keys()))


def unique_types_for_crop_destroy(rows: List[Dict[str, Any]], crop: str, kind: str) -> List[str]:
    s = {}
    ck = crop_key_for_dedup(crop)
    kindN = normalize_text(kind)
    for r in rows:
        cropCol = get_val(r,'crops')
        kindsCol = get_val(r,'destroy')
        typeCol = get_val(r,'type')
        if not kindsCol or not typeCol:
            continue
        options = [crop_key_for_dedup(x) for x in split_crops_field(str(cropCol))]
        if ck not in options:
            continue
        kinds = [normalize_text(x) for x in str(kindsCol).split(',')]
        if kindN in kinds:
            s[title_case(typeCol)] = True
    return sorted(list(s.keys()))


def filter_by_crop_type_destroy(rows: List[Dict[str, Any]], crop: str, typ: str, kind: str) -> List[Dict[str, Any]]:
    cropK = crop_key_for_dedup(crop)
    typeN = normalize_text(typ)
    kindN = normalize_text(kind)
    out = []
    for r in rows:
        cropCol = get_val(r,'crops')
        typeCol = get_val(r,'type')
        kindsCol = get_val(r,'destroy')
        if not kindsCol:
            continue
        options = [crop_key_for_dedup(x) for x in split_crops_field(str(cropCol))]
        kinds = [normalize_text(x) for x in str(kindsCol).split(',')]
        if (cropK in options) and (normalize_text(typeCol).find(typeN) >= 0) and (kindN in kinds):
            out.append(r)
    return out


_CROPS_CACHE: Dict[str, Any] = {'list': [], 'map': {}}


def build_crops_index(rows: List[Dict[str, Any]]):
    m = {}
    for r in rows:
        cropCol = get_val(r,'crops')
        for c in split_crops_field(str(cropCol)):
            k = crop_key_for_dedup(c)
            if k not in m:
                m[k] = pretty_crop_label(c)
    lst = sorted(m.values(), key=lambda x: x)
    _CROPS_CACHE['list'] = lst
    _CROPS_CACHE['map'] = { hash32(crop_key_for_dedup(x)): x for x in lst }


# ============================================================================
# CALCULATOR FUNCTIONS
# ============================================================================

# --- Calculator helpers ported from Code.gs ---

def get_pesticides_for_culture_and_mode(rows: List[Dict[str, Any]], culture: str, mode: str) -> List[Dict[str, Any]]:
    """Filter products by culture and measure required by mode (га for area/tank, т for seed)."""
    measure_needed = 'т' if mode == 'seed' else 'га'
    ck = crop_key_for_dedup(culture)
    out: List[Dict[str, Any]] = []
    for r in rows:
        crop_col = get_val(r, 'crops')
        rate = get_val(r, 'rate')
        if not crop_col or not rate:
            continue
        opts = [crop_key_for_dedup(x) for x in split_crops_field(str(crop_col))]
        if ck not in opts:
            continue
        comps = parse_rate_components(rate)
        if not comps:
            continue
        if comps[0].get('measure') == measure_needed:
            out.append(r)
    return out


def list_crops_for_mode(rows: List[Dict[str, Any]], mode: str) -> List[str]:
    """Return deduplicated pretty crop labels that have at least one product with proper measure for mode."""
    measure_needed = 'т' if mode == 'seed' else 'га'
    seen = {}
    out: List[str] = []
    for r in rows:
        crop_col = get_val(r, 'crops')
        rate = get_val(r, 'rate')
        if not crop_col or not rate:
            continue
        comps = parse_rate_components(rate)
        if not comps or comps[0].get('measure') != measure_needed:
            continue
        for c in split_crops_field(str(crop_col)):
            key = crop_key_for_dedup(c)
            if key not in seen:
                seen[key] = True
                out.append(pretty_crop_label(c))
    out.sort()
    return out


def crops_page_keyboard_for_mode(mode: str, page: int = 0, per: int = 22) -> InlineKeyboardMarkup:
    """Inline keyboard for crop selection filtered by mode."""
    data = _DATA_CACHE.get('data') or {'rows': []}
    rows = data.get('rows', [])
    lst = list_crops_for_mode(rows, mode)
    total = len(lst)
    if total == 0:
        return InlineKeyboardMarkup([])
    pages = max(1, (total + per - 1)//per)
    page = max(0, min(page, pages-1))
    start = page*per
    slice_ = lst[start:start+per]

    rows_kb = create_smart_keyboard(
        slice_,
        lambda label: InlineKeyboardButton(
            text=label,
            callback_data=f'calccrop|m:{mode}|h:{hash32(crop_key_for_dedup(label))}'
        )
    )

    if pages > 1:
        nav: List[InlineKeyboardButton] = []
        if page > 0:
            nav.append(InlineKeyboardButton(text='⬅️ Назад', callback_data=f'calccroppg|m:{mode}|{page-1}'))
        nav.append(InlineKeyboardButton(text=f'{page+1}/{pages}', callback_data='noop'))
        if page < pages-1:
            nav.append(InlineKeyboardButton(text='Вперёд ➡️', callback_data=f'calccroppg|m:{mode}|{page+1}'))
        rows_kb.append(nav)

    return InlineKeyboardMarkup(rows_kb)


def all_products_keyboard(mode: str, culture: str, rows: List[Dict[str, Any]], page: int = 0, per: int = 20) -> InlineKeyboardMarkup:
    items = get_pesticides_for_culture_and_mode(rows, culture, mode)
    names = [get_val(r, 'name') for r in items if get_val(r, 'name')]
    total = len(names)
    if total == 0:
        return InlineKeyboardMarkup([])
    pages = max(1, (total + per - 1)//per)
    page = max(0, min(page, pages-1))
    start = page*per
    slice_ = names[start:start+per]

    def btn_builder(name: str) -> InlineKeyboardButton:
        return InlineKeyboardButton(
            text=name,
            callback_data=f"calc|pick|m:{mode}|h:{hash32(normalize_text(name))}"
        )

    kb_rows = create_smart_keyboard(slice_, btn_builder)
    # nav
    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'calc|allprods|m:{mode}|p:{page-1}'))
    nav.append(InlineKeyboardButton(f'{page+1}/{pages}', callback_data='noop'))
    if page < pages-1:
        nav.append(InlineKeyboardButton('Вперёд ➡️', callback_data=f'calc|allprods|m:{mode}|p:{page+1}'))
    kb_rows.append(nav)
    return InlineKeyboardMarkup(kb_rows)

def parse_rate_components(rate_string: str) -> List[Dict[str, Any]]:
    """
    Parse rate string into components.
    
    Examples:
    "0,5–0,7 л/га" -> [{'name': 'препарату', 'min_rate': 0.5, 'max_rate': 0.7, 'unit': 'л', 'measure': 'га', 'precision': 1}]
    "70 мл/га" -> [{'name': 'препарату', 'min_rate': 70.0, 'max_rate': 70.0, 'unit': 'мл', 'measure': 'га', 'precision': 0}]
    "0,25 кг/га + ПАВ Контур 0,1 л/га" -> [{'name': 'препарату', ...}, {'name': 'ПАВ Контур', ...}]
    """
    import re
    
    if not rate_string or not rate_string.strip():
        return []
    
    # Split by '+' to handle multiple components
    components = [c.strip() for c in rate_string.split('+') if c.strip()]
    result = []
    
    for i, component in enumerate(components):
        # Pattern to match: [name] number[-number] unit/measure
        # Examples: "0,5–0,7 л/га", "ПАВ Контур 0,1 л/га", "70 мл/га"
        pattern = r'^(?:(.+?)\s+)?([0-9]+(?:[,.]\d+)?)(?:[–—-]([0-9]+(?:[,.]\d+)?))?\s*([а-яё]+)/([а-яё]+)$'
        match = re.match(pattern, component.strip(), re.IGNORECASE)
        
        if not match:
            continue
            
        name_part = match.group(1)
        min_rate_str = match.group(2)
        max_rate_str = match.group(3)
        unit = match.group(4)
        measure = match.group(5)
        
        # Convert comma to dot for float parsing
        min_rate_str = min_rate_str.replace(',', '.')
        min_rate = float(min_rate_str)
        
        if max_rate_str:
            max_rate_str = max_rate_str.replace(',', '.')
            max_rate = float(max_rate_str)
        else:
            max_rate = min_rate
            
        # Determine precision from original string
        original_min = match.group(2)
        if ',' in original_min:
            precision = len(original_min.split(',')[1])
        elif '.' in original_min:
            precision = len(original_min.split('.')[1])
        else:
            precision = 0
            
        # Set name
        if name_part and name_part.strip():
            name = name_part.strip()
        else:
            name = 'препарату'
            
        result.append({
            'name': name,
            'min_rate': min_rate,
            'max_rate': max_rate,
            'unit': unit,
            'measure': measure,
            'precision': precision
        })
    
    return result


def smart_convert(value: float, unit: str) -> Tuple[float, str]:
    """
    Convert small units to larger units when appropriate.
    Returns a tuple: (converted_value, unit)
    """
    u = (unit or '').lower()
    if u == 'мл' and value >= 1000:
        return (value / 1000.0, 'л')
    if u == 'г' and value >= 1000:
        return (value / 1000.0, 'кг')
    return (value, unit)


def format_number(num: float, precision: int = 2) -> str:
    """Format number with given precision, removing unnecessary zeros."""
    if precision == 0:
        return str(int(round(num)))
    formatted = f"{num:.{precision}f}"
    # Remove trailing zeros
    if '.' in formatted:
        formatted = formatted.rstrip('0').rstrip('.')
    return formatted


def parse_number(s: str) -> float:
    """Parse a number from a string, accepting comma as decimal separator."""
    try:
        import re
        s = str(s or '').strip().replace(' ', '').replace(',', '.')
        m = re.search(r'[-+]?\d+(?:\.\d+)?', s)
        return float(m.group()) if m else float('nan')
    except Exception:
        return float('nan')


def format_num_prec(num: float, precision: int) -> str:
    """Format number with exact precision from Code.gs."""
    if precision <= 0:
        return str(int(round(num)))
    
    formatted = f"{num:.{precision}f}"
    return formatted




def calculate_for_area(rate_components: List[Dict[str, Any]], hectares: float) -> List[Dict[str, Any]]:
    """
    Calculate total amount of pesticide for given area.
    
    Works only for components with measure == 'га'.
    """
    result = []
    
    for component in rate_components:
        if component.get('measure') != 'га':
            continue
            
        min_total = component['min_rate'] * hectares
        max_total = component['max_rate'] * hectares
        
        # Apply smart conversion
        min_converted, unit_min = smart_convert(min_total, component['unit'])
        max_converted, unit_max = smart_convert(max_total, component['unit'])
        
        # Use the unit from max conversion (should be same as min)
        final_unit = unit_max
        
        result.append({
            'name': component['name'],
            'min_total': min_converted,
            'max_total': max_converted,
            'unit': final_unit
        })
    
    return result


def calculate_for_tank(rate_components: List[Dict[str, Any]], water_rate_per_ha: float, tank_volume: float) -> Dict[str, Any]:
    """
    Calculate amount of pesticide for one tank filling.
    
    Works only for components with measure == 'га'.
    """
    ha_per_tank = tank_volume / water_rate_per_ha
    components_result = []
    
    for component in rate_components:
        if component.get('measure') != 'га':
            continue
            
        min_per_tank = component['min_rate'] * ha_per_tank
        max_per_tank = component['max_rate'] * ha_per_tank
        
        # Apply smart conversion
        min_converted, unit_min = smart_convert(min_per_tank, component['unit'])
        max_converted, unit_max = smart_convert(max_per_tank, component['unit'])
        
        # Use the unit from max conversion
        final_unit = unit_max
        
        components_result.append({
            'name': component['name'],
            'min_total': min_converted,
            'max_total': max_converted,
            'unit': final_unit
        })
    
    return {
        'ha_per_tank': ha_per_tank,
        'components': components_result
    }


def calculate_for_seed(rate_components: List[Dict[str, Any]], tons: float) -> List[Dict[str, Any]]:
    """
    Calculate total amount of seed treatment for given tons of seeds.
    
    Works only for components with measure == 'т'.
    """
    result = []
    
    for component in rate_components:
        if component.get('measure') != 'т':
            continue
            
        min_total = component['min_rate'] * tons
        max_total = component['max_rate'] * tons
        
        # Apply smart conversion
        min_converted, unit_min = smart_convert(min_total, component['unit'])
        max_converted, unit_max = smart_convert(max_total, component['unit'])
        
        # Use the unit from max conversion
        final_unit = unit_max
        
        result.append({
            'name': component['name'],
            'min_total': min_converted,
            'max_total': max_converted,
            'unit': final_unit
        })
    
    return result


def apply_custom_rate(rate_components: List[Dict[str, Any]], custom_rate: float) -> List[Dict[str, Any]]:
    """
    Apply custom rate to the first component.
    
    Modifies the first component's min_rate and max_rate to custom_rate.
    """
    if not rate_components:
        return rate_components
        
    # Create a copy to avoid modifying the original
    result = []
    for i, component in enumerate(rate_components):
        new_component = component.copy()
        if i == 0:  # First component gets custom rate
            new_component['min_rate'] = custom_rate
            new_component['max_rate'] = custom_rate
        result.append(new_component)
    
    return result


# ============================================================================
# STATE MANAGEMENT SYSTEM
# ============================================================================

# State constants
STATE_NONE = None
STATE_AWAITING_NAME = 'awaiting_name'
STATE_AWAITING_DV = 'awaiting_dv'
STATE_CALC_MODE = 'calculator_awaiting_mode'
STATE_CALC_CROP = 'calculator_awaiting_crop'
STATE_CALC_HECTARES = 'calculator_awaiting_hectares'
STATE_CALC_WATER_RATE = 'calculator_awaiting_water_rate'
STATE_CALC_TANK_VOLUME = 'calculator_awaiting_tank_volume'
STATE_CALC_TONS = 'calculator_awaiting_tons'
STATE_CALC_PESTICIDE_SELECT = 'calculator_pesticide_select'
STATE_CALC_WATER_RATE_INPUT = 'calculator_water_rate_input'
STATE_CALC_TANK_VOLUME_INPUT = 'calculator_tank_volume_input'
STATE_CALC_AMOUNT_INPUT = 'calculator_amount_input'
STATE_CALC_CUSTOM_RATE_INPUT = 'calculator_custom_rate_input'


def clear_user_state(context: ContextTypes.DEFAULT_TYPE):
    """Clear all user state data."""
    context.user_data.clear()


def set_user_state(context: ContextTypes.DEFAULT_TYPE, state: str, **kwargs):
    """Set user state and optional data."""
    context.user_data['state'] = state
    for key, value in kwargs.items():
        context.user_data[key] = value


def get_user_state(context: ContextTypes.DEFAULT_TYPE) -> str:
    """Get current user state."""
    return context.user_data.get('state', STATE_NONE)


def format_calculation_result(components: List[Dict[str, Any]], title: str = "") -> str:
    """Format calculation results into a readable message."""
    if not components:
        return "❌ Нет данных для расчета"
    
    lines = []
    if title:
        lines.append(f"📊 <b>{title}</b>\n")
    
    for comp in components:
        name = comp.get('name', 'препарату')
        min_total = comp.get('min_total', 0)
        max_total = comp.get('max_total', 0)
        unit = comp.get('unit', '')
        
        if min_total == max_total:
            amount_str = format_number(min_total, 2 if min_total < 1 else 1)
        else:
            min_str = format_number(min_total, 2 if min_total < 1 else 1)
            max_str = format_number(max_total, 2 if max_total < 1 else 1)
            amount_str = f"{min_str}–{max_str}"
        
        if name == 'препарату':
            lines.append(f"💧 По {name}: <b>{amount_str} {unit}</b>")
        else:
            lines.append(f"➕ {name}: <b>{amount_str} {unit}</b>")
    
    return "\n".join(lines)


def format_tank_calculation_result(result: Dict[str, Any], title: str = "") -> str:
    """Format tank calculation results into a readable message."""
    if not result or not result.get('components'):
        return "❌ Нет данных для расчета"
    
    lines = []
    if title:
        lines.append(f"📊 <b>{title}</b>\n")
    
    ha_per_tank = result.get('ha_per_tank', 0)
    lines.append(f"🚜 Площадь на бак: <b>{format_number(ha_per_tank, 1)} га</b>\n")
    
    for comp in result['components']:
        name = comp.get('name', 'препарату')
        min_total = comp.get('min_total', 0)
        max_total = comp.get('max_total', 0)
        unit = comp.get('unit', '')
        
        if min_total == max_total:
            amount_str = format_number(min_total, 2 if min_total < 1 else 1)
        else:
            min_str = format_number(min_total, 2 if min_total < 1 else 1)
            max_str = format_number(max_total, 2 if max_total < 1 else 1)
            amount_str = f"{min_str}–{max_str}"
        
        if name == 'препарату':
            lines.append(f"💧 По {name}: <b>{amount_str} {unit}</b>")
        else:
            lines.append(f"➕ {name}: <b>{amount_str} {unit}</b>")
    
    return "\n".join(lines)


def format_calculator_result_card(mode: str, culture: str, pesticide_name: str, rate_str: str, amount: float, result: Any) -> str:
    """Unified formatter to mirror Code.gs card-style output for all calculator modes."""
    header_lines: List[str] = []
    header_lines.append(f"🌱 <i>Культура:</i> <b>{culture}</b>")
    header_lines.append(f"📦 <i>Препарат:</i> <b>{pesticide_name}</b>")
    if rate_str:
        header_lines.append(f"💧 <i>Норма:</i> <b>{rate_str}</b>")

    if mode == 'tank':
        title = f"Расчёт для бака {format_number(amount, 0)} л"
        body = format_tank_calculation_result(result, title)
    elif mode == 'area':
        title = f"Расчёт для {format_number(amount, 1)} га"
        body = format_calculation_result(result, title)
    else:  # seed
        title = f"Протравливание {format_number(amount, 1)} т семян"
        body = format_calculation_result(result, title)

    return "\n".join(header_lines) + "\n\n" + body


def build_combined_product_card(product_name: str, rows: List[Dict[str, Any]]) -> str:
    """Build a single card for a product, merging cultures across all rows with that name."""
    nkey = normalize_text(product_name)
    same = [r for r in rows if get_val(r,'name') and normalize_text(get_val(r,'name')) == nkey]
    if not same:
        return f"🛡️ <b>{product_name}</b>\n❌ Данных не найдено"
    # Take type/ai/rate from the first non-empty
    typ = next((get_val(r,'type') for r in same if get_val(r,'type')), '')
    ai = next((get_val(r,'ai') for r in same if get_val(r,'ai')), '')
    rate = next((get_val(r,'rate') for r in same if get_val(r,'rate')), '')
    # Merge cultures
    cultures = []
    seen = set()
    for r in same:
        crops = get_val(r, 'crops')
        for c in split_crops_field(crops):
            cc = normalize_crop_name(c)
            ck = crop_key_for_dedup(cc)
            if ck not in seen:
                seen.add(ck)
                cultures.append(cc)
    cultures_text = ', '.join(cultures)
    parts = [f"🛡️ <b>{product_name}</b>"]
    if typ:
        parts.append(f"🏷️ Вид: <b>{typ}</b>")
    if ai:
        parts.append(f"🧪 Д.в.: <b>{ai}</b>")
    if cultures_text:
        parts.append(f"🌱 Культура: <b>{cultures_text}</b>")
    if rate:
        parts.append(f"💧 Норма: <b>{rate}</b>")
    return "\n".join(parts)


def create_smart_keyboard(items: List[str], callback_func) -> List[List[InlineKeyboardButton]]:
    """
    Smart button grouping: long labels occupy full row, short labels go two per row.
    Preserves order but groups short elements for compactness.
    """
    rows = []
    MAX_BUTTON_LENGTH = 20  # If longer than 20 chars - full row (for mobile screen)
    
    # Create all buttons with indices
    buttons = []
    for i, item in enumerate(items):
        btn = callback_func(item)
        buttons.append({
            'btn': btn,
            'text': btn.text or '',
            'index': i,
            'used': False
        })
    
    # Group with smart logic
    for i in range(len(buttons)):
        if buttons[i]['used']:
            continue
        
        current = buttons[i]
        current['used'] = True
        
        # If long - full row
        if len(current['text']) > MAX_BUTTON_LENGTH:
            rows.append([current['btn']])
        else:
            # Short - look for pair
            row = [current['btn']]
            
            # Look for next short button (not necessarily immediately next)
            for j in range(i + 1, len(buttons)):
                if buttons[j]['used']:
                    continue
                if len(buttons[j]['text']) <= MAX_BUTTON_LENGTH:
                    row.append(buttons[j]['btn'])
                    buttons[j]['used'] = True
                    break
            
            rows.append(row)
    
    return rows


def crops_page_keyboard(page: int = 0, per: int = 22) -> InlineKeyboardMarkup:
    total = len(_CROPS_CACHE['list'])
    if total == 0:
        return InlineKeyboardMarkup([])
    pages = max(1, (total + per - 1)//per)
    page = max(0, min(page, pages-1))
    start = page*per
    slice_ = _CROPS_CACHE['list'][start:start+per]
    
    # Use smart keyboard grouping
    rows = create_smart_keyboard(
        slice_,
        lambda label: InlineKeyboardButton(
            text=label, 
            callback_data=f'crop|h:{hash32(crop_key_for_dedup(label))}'
        )
    )
    
    # Add navigation
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text='⬅️ Назад', callback_data=f'croppg|{page-1}'))
        nav.append(InlineKeyboardButton(text=f'{page+1}/{pages}', callback_data='noop'))
        if page < pages-1:
            nav.append(InlineKeyboardButton(text='Вперёд ➡️', callback_data=f'croppg|{page+1}'))
        rows.append(nav)
    
    return InlineKeyboardMarkup(rows)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger = logging.getLogger(__name__)
    chat_id = update.effective_chat.id if update.effective_chat else None
    logger.info(f"Получена команда /start от пользователя {chat_id}")
    
    welcome = '👋 <b>Добро пожаловать!</b>\nЭтот бот поможет быстро подобрать пестицид по вашей культуре и вредному объекту, а также найти препарат по названию. Выберите действие на клавиатуре ниже.'
    if chat_id:
        try:
            logger.info("Загружаем данные...")
            await ensure_data_loaded()
            data = _DATA_CACHE['data']
            build_crops_index(data['rows'])
            logger.info("Отправляем приветственное сообщение...")
            await context.bot.send_message(chat_id=chat_id, text=welcome, parse_mode='HTML', reply_markup=reply_kb())
            logger.info("Приветственное сообщение отправлено успешно")
        except Exception as e:
            logger.error(f"Ошибка в cmd_start: {e}")
            try:
                await context.bot.send_message(chat_id=chat_id, text="❌ Произошла ошибка при запуске бота")
            except:
                pass


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id:
        await context.bot.send_message(chat_id=chat_id, text='📋 Главное меню', reply_markup=reply_kb())


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    help_text = 'ℹ️ <b>Как пользоваться</b>\n• Нажмите "Подбор пестицида" → выберите культуру/цели обработки → выберите вид объекта → выберите вид препарата.\n• Или отправьте название препарата — я подберу ближайшие совпадения.\n• Нажмите "Поиск по д.в." — введите действующее вещество и получите список препаратов.'
    if chat_id:
        await context.bot.send_message(chat_id=chat_id, text=help_text, parse_mode='HTML', reply_markup=reply_kb())


async def cmd_setcommands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    commands = [
        BotCommand('start', 'Перезапуск / Главное меню'),
        BotCommand('menu', 'Показать клавиатуру меню'),
        BotCommand('reload', 'Обновить данные из таблицы'),
        BotCommand('help', 'Справка по использованию')
    ]
    await context.bot.set_my_commands(commands=commands)
    if chat_id:
        await context.bot.send_message(chat_id=chat_id, text='Меню команд установлено')


async def cmd_reload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    _DATA_CACHE['expires'] = 0
    _CONTACTS_CACHE['expires'] = 0
    await ensure_data_loaded(force=True)
    data = _DATA_CACHE['data']
    build_crops_index(data['rows'])
    if chat_id:
        await context.bot.send_message(chat_id=chat_id, text='Кеш обновлён. Клавиатура обновлена.', parse_mode='HTML', reply_markup=reply_kb())


async def cmd_dbg_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    os.environ['DEBUG'] = '1'
    if chat_id:
        await context.bot.send_message(chat_id=chat_id, text='DEBUG=1')


async def cmd_dbg_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    os.environ['DEBUG'] = '0'
    if chat_id:
        await context.bot.send_message(chat_id=chat_id, text='DEBUG=0')


async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger = logging.getLogger(__name__)
    msg = update.effective_message
    if not msg or not msg.text:
        return
    chat_id = update.effective_chat.id if update.effective_chat else None
    text = msg.text
    logger.info(f"Получено сообщение от {chat_id}: {text[:50]}...")
    btn = clean_btn(text)
    if text == '/start' or text == '/restart':
        await cmd_start(update, context)
        return
    if text == '/menu':
        await cmd_menu(update, context)
        return
    if text == '/help':
        await cmd_help(update, context)
        return
    if text.startswith('/setcommands'):
        await cmd_setcommands(update, context)
        return
    if text.startswith('/reload'):
        await cmd_reload(update, context)
        return
    if text == '/dbg_on':
        await cmd_dbg_on(update, context)
        return
    if text == '/dbg_off':
        await cmd_dbg_off(update, context)
        return

    # Preempt menu buttons regardless of current state
    if btn == 'подбор пестицида':
        clear_user_state(context)
        await ensure_data_loaded()
        data = _DATA_CACHE['data']
        build_crops_index(data['rows'])
        if chat_id:
            await context.bot.send_message(chat_id=chat_id, text='📋 <b>Выберите культуру/цели обработки</b>', parse_mode='HTML', reply_markup=crops_page_keyboard(0))
        return
    if btn == 'поиск препарата по названию':
        clear_user_state(context)
        set_user_state(context, STATE_AWAITING_NAME)
        if chat_id:
            await context.bot.send_message(chat_id=chat_id, text='🔎 Введите название препарата текстом. Я учту опечатки и раскладку.', reply_markup=reply_kb())
        return
    if btn == 'поиск по дв':
        clear_user_state(context)
        set_user_state(context, STATE_AWAITING_DV)
        if chat_id:
            await context.bot.send_message(chat_id=chat_id, text='🧪 Введите часть названия действующего вещества (например: "флорасулам" или "д.в. 2,4-д")', reply_markup=reply_kb())
        return
    if btn == 'калькулятор расхода препарата':
        clear_user_state(context)
        if chat_id:
            calc_menu = '🧮 Выберите режим расчёта'
            rows_calc = [
                [InlineKeyboardButton('Рассчитать по площади', callback_data='calc|mode|area')],
                [InlineKeyboardButton('Рассчитать по объёму бака', callback_data='calc|mode|tank')],
                [InlineKeyboardButton('Рассчитать норму протравителя', callback_data='calc|mode|seed')]
            ]
            await context.bot.send_message(
                chat_id=chat_id,
                text=calc_menu,
                reply_markup=InlineKeyboardMarkup(rows_calc)
            )
        return
    if btn == 'помощь':
        clear_user_state(context)
        await cmd_help(update, context)
        return
    if btn == 'контакты':
        contacts = await ensure_contacts_loaded()
        if chat_id:
            if not contacts:
                await context.bot.send_message(chat_id=chat_id, text='❌ Контакты не найдены', reply_markup=reply_kb())
                return
            buttons: List[InlineKeyboardButton] = []
            for idx, c in enumerate(contacts):
                label = c.get('Филиал/Офис') or 'Офис'
                buttons.append(InlineKeyboardButton(text=label, callback_data=f'contact|{idx}'))
            rows_kb: List[List[InlineKeyboardButton]] = []
            cur: List[InlineKeyboardButton] = []
            for b in buttons:
                if len(b.text) > 18:
                    if cur:
                        rows_kb.append(cur)
                        cur = []
                    rows_kb.append([b])
                else:
                    cur.append(b)
                    if len(cur) == 2:
                        rows_kb.append(cur)
                        cur = []
            if cur:
                rows_kb.append(cur)
            await context.bot.send_message(chat_id=chat_id, text='📞 <b>Выберите филиал/офис:</b>', parse_mode='HTML', reply_markup=InlineKeyboardMarkup(rows_kb))
        return

    # Handle state-based input processing
    current_state = get_user_state(context)
    if current_state and not text.startswith('/'):
        try:
            if current_state == STATE_AWAITING_NAME:
                await ensure_data_loaded()
                rows = _DATA_CACHE['data']['rows']
                q = text
                def score_row(r):
                    name = get_val(r, 'name')
                    if not name:
                        return 0.0
                    import difflib
                    a = normalize_text(q)
                    b = normalize_text(name)
                    return difflib.SequenceMatcher(None, a, b).ratio()
                ranked = sorted(rows, key=score_row, reverse=True)
                top = [r for r in ranked[:10] if score_row(r) > 0.3]
                if not top:
                    await msg.reply_text('Ничего не нашлось. Попробуйте другое написание.', reply_markup=reply_kb())
                else:
                    chunks = []
                    for r in top:
                        name = get_val(r,'name')
                        typ = get_val(r,'type')
                        ai = get_val(r,'ai')
                        rate = get_val(r,'rate')
                        crops = get_val(r,'crops')
                        line = []
                        if name:
                            line.append('🛡️ <b>'+name+'</b>')
                        if typ:
                            line.append('🏷️ Вид: <b>'+typ+'</b>')
                        if ai:
                            line.append('🧪 Д.в.: <b>'+ai+'</b>')
                        if crops:
                            line.append('🌱 Культура: <b>'+crops+'</b>')
                        if rate:
                            line.append('💧 Норма: <b>'+rate+'</b>')
                        chunks.append('\n'.join(line))
                    await msg.reply_html(('\n\n').join(chunks), reply_markup=reply_kb())
                clear_user_state(context)
                return
                
            elif current_state == STATE_AWAITING_DV:
                await ensure_data_loaded()
                rows = _DATA_CACHE['data']['rows']
                q = normalize_text(text)
                res = []
                for r in rows:
                    ai = get_val(r, 'ai')
                    if ai and q and normalize_text(ai).find(q) >= 0:
                        res.append(r)
                res = res[:10]
                if not res:
                    await msg.reply_text('Не найдено по д.в.', reply_markup=reply_kb())
                else:
                    chunks = []
                    for r in res:
                        name = get_val(r,'name')
                        ai = get_val(r,'ai')
                        rate = get_val(r,'rate')
                        typ = get_val(r,'type')
                        crops = get_val(r,'crops')
                        line = []
                        if name:
                            line.append('🛡️ <b>'+name+'</b>')
                        if typ:
                            line.append('🏷️ Вид: <b>'+typ+'</b>')
                        if ai:
                            line.append('🧪 Д.в.: <b>'+ai+'</b>')
                        if crops:
                            line.append('🌱 Культура: <b>'+crops+'</b>')
                        if rate:
                            line.append('💧 Норма: <b>'+rate+'</b>')
                        chunks.append('\n'.join(line))
                    await msg.reply_html(('\n\n').join(chunks), reply_markup=reply_kb())
                clear_user_state(context)
                return
                
            # Enhanced calculator states
            elif current_state == STATE_CALC_PESTICIDE_SELECT:
                # Handle text input for pesticide search during enhanced calculator
                culture = context.user_data.get('culture')
                calc_mode = context.user_data.get('calc_mode')
                
                if not culture or not calc_mode:
                    await msg.reply_text('❌ Неверное состояние. Начните заново.', reply_markup=reply_kb())
                    clear_user_state(context)
                    return
                
                await ensure_data_loaded()
                rows = _DATA_CACHE['data']['rows']
                pesticides = get_pesticides_for_culture_and_mode(rows, culture, calc_mode)
                
                # Fuzzy search with layout/translit
                import difflib
                q0 = normalize_text(text)
                to_en, to_ru = switch_layout(text)
                variants = {q0, normalize_text(to_en), normalize_text(to_ru), normalize_text(translit_simple(text))}
                def score_name(name: str) -> float:
                    b = normalize_text(name)
                    return max(difflib.SequenceMatcher(None, v, b).ratio() for v in variants)
                scored = [(p, score_name(get_val(p,'name'))) for p in pesticides if get_val(p,'name')]
                scored.sort(key=lambda x: x[1], reverse=True)
                matches = [p for p,s in scored if s >= 0.55][:10]
                
                if len(matches) == 1:
                    # Exact match found
                    selected_pesticide = matches[0]
                    pesticide_name = get_val(selected_pesticide, 'name')
                    rate_str = get_val(selected_pesticide, 'rate')
                    
                    if not rate_str:
                        await msg.reply_text('❌ У этого препарата не указана норма расхода', reply_markup=reply_kb())
                        return
                    
                    components = parse_rate_components(rate_str)
                    if not components:
                        await msg.reply_text('❌ Не удалось распознать норму расхода', reply_markup=reply_kb())
                        return
                    
                    # Set state for amount input
                    if calc_mode == 'tank':
                        set_user_state(context, STATE_CALC_WATER_RATE_INPUT, 
                                      calc_mode=calc_mode, culture=culture, 
                                      pesticide_name=pesticide_name, rate_str=rate_str, 
                                      components=components)
                        prompt = '💦 Укажите норму раствора (воды) на 1 га, например: 200'
                    else:
                        set_user_state(context, STATE_CALC_AMOUNT_INPUT, 
                                      calc_mode=calc_mode, culture=culture, 
                                      pesticide_name=pesticide_name, rate_str=rate_str, 
                                      components=components)
                        if calc_mode == 'area':
                            prompt = '📏 Укажите площадь обработки в га, например: 50'
                        elif calc_mode == 'seed':
                            prompt = '🌾 Укажите количество семян в тоннах, например: 10'
                        else:
                            prompt = 'Введите количество'
                    
                    message_text = (
                        '✅ <b>Препарат выбран</b>\n\n'
                        f'🌱 <i>Культура:</i> <b>{culture}</b>\n'
                        f'📦 <i>Препарат:</i> <b>{pesticide_name}</b>\n\n'
                        f'{prompt}'
                    )
                    await msg.reply_html(message_text, reply_markup=reply_kb())
                    
                elif len(matches) > 1:
                    # Multiple matches - show options
                    pesticide_names = [get_val(p, 'name') for p in matches]
                    keyboard_rows = create_smart_keyboard(
                        pesticide_names,
                        lambda name: InlineKeyboardButton(
                            text=name,
                            callback_data=f"calc|pick|m:{calc_mode}|h:{hash32(normalize_text(name))}"
                        )
                    )
                    # Add "All products" row
                    keyboard_rows.append([InlineKeyboardButton('📋 Все доступные препараты', callback_data=f'calc|allprods|m:{calc_mode}|p:0')])
                    reply_markup = InlineKeyboardMarkup(keyboard_rows)
                    
                    await msg.reply_html(
                        '🔍 <b>Найдено несколько препаратов:</b>\n\nВыберите нужный:',
                        reply_markup=reply_markup
                    )
                else:
                    # No matches — suggest full list
                    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton('📋 Все доступные препараты', callback_data=f'calc|allprods|m:{calc_mode}|p:0')]])
                    await msg.reply_text(
                        '❌ Препарат не найден для этой культуры. Нажмите кнопку ниже, чтобы увидеть список доступных.',
                        reply_markup=keyboard
                    )
                return
            
            elif current_state == STATE_CALC_WATER_RATE_INPUT:
                try:
                    water_rate = float(text.replace(',', '.'))
                    components = context.user_data.get('components', [])
                    culture = context.user_data.get('culture')
                    pesticide_name = context.user_data.get('pesticide_name')
                    rate_str = context.user_data.get('rate_str')
                    
                    set_user_state(context, STATE_CALC_TANK_VOLUME_INPUT, 
                                  calc_mode='tank', culture=culture, 
                                  pesticide_name=pesticide_name, rate_str=rate_str,
                                  components=components, water_rate=water_rate)
                    
                    await msg.reply_text('🚜 Введите объем бака опрыскивателя (л), например: 3000', reply_markup=reply_kb())
                except ValueError:
                    await msg.reply_text('Введите корректную норму воды (например: 200 или 150.5):', reply_markup=reply_kb())
                return
            
            elif current_state == STATE_CALC_TANK_VOLUME_INPUT:
                try:
                    tank_volume = float(text.replace(',', '.'))
                    components = context.user_data.get('components', [])
                    water_rate = context.user_data.get('water_rate', 200)
                    culture = context.user_data.get('culture')
                    pesticide_name = context.user_data.get('pesticide_name')
                    rate_str = context.user_data.get('rate_str')
                    
                    result = calculate_for_tank(components, water_rate, tank_volume)
                    result['water_rate'] = water_rate
                    result['tank_volume'] = tank_volume
                    
                    if result and result.get('components'):
                        msg_text = format_calculator_result_card('tank', culture, pesticide_name, rate_str, tank_volume, result)
                        
                        # Add "Other rate" button
                        keyboard = [[InlineKeyboardButton('🔄 Другая норма', callback_data='calc|other_rate')]]
                        reply_markup = InlineKeyboardMarkup(keyboard)
                        
                        await msg.reply_html(msg_text, reply_markup=reply_markup)
                    else:
                        await msg.reply_text('❌ Не удалось выполнить расчет', reply_markup=reply_kb())
                    
                    clear_user_state(context)
                except ValueError:
                    await msg.reply_text('Введите корректный объем бака (например: 3000 или 1500.5):', reply_markup=reply_kb())
                return
            
            elif current_state == STATE_CALC_AMOUNT_INPUT:
                try:
                    amount = float(text.replace(',', '.'))
                    components = context.user_data.get('components', [])
                    calc_mode = context.user_data.get('calc_mode')
                    culture = context.user_data.get('culture')
                    pesticide_name = context.user_data.get('pesticide_name')
                    rate_str = context.user_data.get('rate_str')
                    
                    if calc_mode == 'area':
                        result = calculate_for_area(components, amount)
                    elif calc_mode == 'seed':
                        result = calculate_for_seed(components, amount)
                    else:
                        await msg.reply_text('❌ Неверный режим расчета', reply_markup=reply_kb())
                        clear_user_state(context)
                        return
                    
                    if result:
                        msg_text = format_calculator_result_card(calc_mode, culture, pesticide_name, rate_str, amount, result)
                        
                        # Add "Other rate" button
                        keyboard = [[InlineKeyboardButton('🔄 Другая норма', callback_data='calc|other_rate')]]
                        reply_markup = InlineKeyboardMarkup(keyboard)
                        
                        await msg.reply_html(msg_text, reply_markup=reply_markup)
                    else:
                        await msg.reply_text('❌ Не удалось выполнить расчет', reply_markup=reply_kb())
                    
                    clear_user_state(context)
                except ValueError:
                    if context.user_data.get('calc_mode') == 'area':
                        await msg.reply_text('Введите корректное число гектаров (например: 50 или 12.5):', reply_markup=reply_kb())
                    elif context.user_data.get('calc_mode') == 'seed':
                        await msg.reply_text('Введите корректное количество тонн (например: 25 или 12.5):', reply_markup=reply_kb())
                    else:
                        await msg.reply_text('Введите корректное число:', reply_markup=reply_kb())
                return
            
            # Legacy calculator states (keep for backward compatibility)
            elif current_state == STATE_CALC_MODE:
                # Calculator mode selection
                mode_text = text.strip().lower()
                if mode_text in ['1', 'площадь', 'поле']:
                    set_user_state(context, STATE_CALC_CROP, calc_mode='area')
                    await msg.reply_text('🌱 Введите название препарата для расчета по площади:', reply_markup=reply_kb())
                elif mode_text in ['2', 'опрыскиватель', 'бак']:
                    set_user_state(context, STATE_CALC_CROP, calc_mode='tank')
                    await msg.reply_text('🌱 Введите название препарата для расчета на опрыскиватель:', reply_markup=reply_kb())
                elif mode_text in ['3', 'протравитель', 'семена']:
                    set_user_state(context, STATE_CALC_CROP, calc_mode='seed')
                    await msg.reply_text('🌱 Введите название препарата для протравливания:', reply_markup=reply_kb())
                else:
                    await msg.reply_text('Выберите режим калькулятора:\n1 - Расчет по площади\n2 - Расчет для опрыскивателя\n3 - Расчет для протравителя', reply_markup=reply_kb())
                return
                
            elif current_state == STATE_CALC_CROP:
                # Find product by name or parse manual rate
                await ensure_data_loaded()
                rows = _DATA_CACHE['data']['rows']
                
                # Try to parse as manual rate first
                components = parse_rate_components(text)
                if components:
                    # Manual rate entered
                    calc_mode = context.user_data.get('calc_mode')
                    product_name = 'Препарат (ручной ввод)'
                    
                    if calc_mode == 'area':
                        set_user_state(context, STATE_CALC_HECTARES, components=components, product_name=product_name)
                        await msg.reply_text(f'📊 {product_name}\n💧 Норма: {text}\n\n🌾 Введите площадь в гектарах:', reply_markup=reply_kb())
                    elif calc_mode == 'tank':
                        set_user_state(context, STATE_CALC_WATER_RATE, components=components, product_name=product_name)
                        await msg.reply_text(f'📊 {product_name}\n💧 Норма: {text}\n\n💦 Введите норму воды (л/га):', reply_markup=reply_kb())
                    elif calc_mode == 'seed':
                        set_user_state(context, STATE_CALC_TONS, components=components, product_name=product_name)
                        await msg.reply_text(f'📊 {product_name}\n💧 Норма: {text}\n\n⚖️ Введите количество тонн семян:', reply_markup=reply_kb())
                    return
                
                # Try to find product by name
                q = normalize_text(text)
                found = None
                for r in rows:
                    name = get_val(r, 'name')
                    if name and normalize_text(name).find(q) >= 0:
                        found = r
                        break
                
                if not found:
                    await msg.reply_text('Препарат не найден. Введите норму вручную в формате "0,5 л/га" или попробуйте другое название:', reply_markup=reply_kb())
                    return
                
                rate_str = get_val(found, 'rate')
                if not rate_str:
                    await msg.reply_text('У этого препарата не указана норма расхода. Введите норму вручную в формате "0,5 л/га":', reply_markup=reply_kb())
                    return
                
                # Parse rate and continue to next step
                components = parse_rate_components(rate_str)
                if not components:
                    await msg.reply_text('Не удалось распознать норму расхода. Введите норму вручную в формате "0,5 л/га":', reply_markup=reply_kb())
                    return
                
                calc_mode = context.user_data.get('calc_mode')
                product_name = get_val(found, 'name')
                
                if calc_mode == 'area':
                    set_user_state(context, STATE_CALC_HECTARES, components=components, product_name=product_name)
                    await msg.reply_text(f'📊 Препарат: {product_name}\n💧 Норма: {rate_str}\n\n🌾 Введите площадь в гектарах:', reply_markup=reply_kb())
                elif calc_mode == 'tank':
                    set_user_state(context, STATE_CALC_WATER_RATE, components=components, product_name=product_name)
                    await msg.reply_text(f'📊 Препарат: {product_name}\n💧 Норма: {rate_str}\n\n💦 Введите норму воды (л/га):', reply_markup=reply_kb())
                elif calc_mode == 'seed':
                    set_user_state(context, STATE_CALC_TONS, components=components, product_name=product_name)
                    await msg.reply_text(f'📊 Препарат: {product_name}\n💧 Норма: {rate_str}\n\n⚖️ Введите количество тонн семян:', reply_markup=reply_kb())
                return
                
            elif current_state == STATE_CALC_HECTARES:
                try:
                    hectares = float(text.replace(',', '.'))
                    components = context.user_data.get('components', [])
                    product_name = context.user_data.get('product_name', 'Препарат')
                    
                    result = calculate_for_area(components, hectares)
                    if result:
                        msg_text = format_calculation_result(result, f"Расчет для {hectares} га")
                        await msg.reply_html(msg_text, reply_markup=reply_kb())
                    else:
                        await msg.reply_text('❌ Не удалось выполнить расчет', reply_markup=reply_kb())
                    clear_user_state(context)
                except ValueError:
                    await msg.reply_text('Введите корректное число гектаров (например: 50 или 12.5):', reply_markup=reply_kb())
                return
                
            elif current_state == STATE_CALC_WATER_RATE:
                try:
                    water_rate = float(text.replace(',', '.'))
                    components = context.user_data.get('components', [])
                    set_user_state(context, STATE_CALC_TANK_VOLUME, components=components, water_rate=water_rate, 
                                  product_name=context.user_data.get('product_name'))
                    await msg.reply_text('🚜 Введите объем бака опрыскивателя (л):', reply_markup=reply_kb())
                except ValueError:
                    await msg.reply_text('Введите корректную норму воды (например: 200 или 150.5):', reply_markup=reply_kb())
                return
                
            elif current_state == STATE_CALC_TANK_VOLUME:
                try:
                    tank_volume = float(text.replace(',', '.'))
                    components = context.user_data.get('components', [])
                    water_rate = context.user_data.get('water_rate', 200)
                    product_name = context.user_data.get('product_name', 'Препарат')
                    
                    result = calculate_for_tank(components, water_rate, tank_volume)
                    if result:
                        msg_text = format_tank_calculation_result(result, f"Расчет для бака {tank_volume} л")
                        await msg.reply_html(msg_text, reply_markup=reply_kb())
                    else:
                        await msg.reply_text('❌ Не удалось выполнить расчет', reply_markup=reply_kb())
                    clear_user_state(context)
                except ValueError:
                    await msg.reply_text('Введите корректный объем бака (например: 3000 или 1500.5):', reply_markup=reply_kb())
                return
                
            elif current_state == STATE_CALC_TONS:
                try:
                    tons = float(text.replace(',', '.'))
                    components = context.user_data.get('components', [])
                    product_name = context.user_data.get('product_name', 'Препарат')
                    
                    result = calculate_for_seed(components, tons)
                    if result:
                        msg_text = format_calculation_result(result, f"Протравливание {tons} т семян")
                        await msg.reply_html(msg_text, reply_markup=reply_kb())
                    else:
                        await msg.reply_text('❌ Не удалось выполнить расчет', reply_markup=reply_kb())
                    clear_user_state(context)
                except ValueError:
                    await msg.reply_text('Введите корректное количество тонн (например: 25 или 12.5):', reply_markup=reply_kb())
                return
            
            elif current_state == STATE_CALC_CUSTOM_RATE_INPUT:
                # Accept a number, update first component and ask for amount again
                try:
                    val = parse_number(text)
                    if val != val:  # NaN
                        raise ValueError
                    comps = context.user_data.get('components', [])
                    comps = apply_custom_rate(comps, val)
                    context.user_data['components'] = comps
                    mode = context.user_data.get('calc_mode', 'area')
                    if mode == 'tank':
                        set_user_state(context, STATE_CALC_WATER_RATE_INPUT, **context.user_data)
                        await msg.reply_text('💦 Укажите норму воды (л/га):', reply_markup=reply_kb())
                    elif mode == 'area':
                        set_user_state(context, STATE_CALC_AMOUNT_INPUT, **context.user_data)
                        await msg.reply_text('📏 Укажите площадь (га):', reply_markup=reply_kb())
                    else:
                        set_user_state(context, STATE_CALC_AMOUNT_INPUT, **context.user_data)
                        await msg.reply_text('🌾 Укажите количество семян (т):', reply_markup=reply_kb())
                except Exception:
                    await msg.reply_text('Введите число, например 0.5', reply_markup=reply_kb())
                return
                
        except Exception as e:
            await msg.reply_text('Произошла ошибка. Попробуйте еще раз.', reply_markup=reply_kb())
            clear_user_state(context)
            return

    # Clear state and handle button presses
    if btn == 'подбор пестицида':
        clear_user_state(context)
        await ensure_data_loaded()
        data = _DATA_CACHE['data']
        build_crops_index(data['rows'])
        if chat_id:
            await context.bot.send_message(chat_id=chat_id, text='📋 <b>Выберите культуру/цели обработки</b>', parse_mode='HTML', reply_markup=crops_page_keyboard(0))
        return
    if btn == 'поиск препарата по названию':
        clear_user_state(context)
        set_user_state(context, STATE_AWAITING_NAME)
        if chat_id:
            await context.bot.send_message(chat_id=chat_id, text='🔎 Введите название препарата текстом. Я учту опечатки и раскладку.', reply_markup=reply_kb())
        return
    if btn == 'поиск по дв':
        clear_user_state(context)
        set_user_state(context, STATE_AWAITING_DV)
        if chat_id:
            await context.bot.send_message(chat_id=chat_id, text='🧪 Введите часть названия действующего вещества (например: "флорасулам" или "д.в. 2,4-д")', reply_markup=reply_kb())
        return
    if btn == 'калькулятор расхода препарата':
        clear_user_state(context)
        if chat_id:
            # Show mode selection with inline buttons like in Code.gs
            calc_menu = '🧮 Выберите режим расчёта'
            rows_calc = [
                [InlineKeyboardButton('Рассчитать по площади', callback_data='calc|mode|area')],
                [InlineKeyboardButton('Рассчитать по объёму бака', callback_data='calc|mode|tank')],
                [InlineKeyboardButton('Рассчитать норму протравителя', callback_data='calc|mode|seed')]
            ]
            await context.bot.send_message(
                chat_id=chat_id, 
                text=calc_menu, 
                reply_markup=InlineKeyboardMarkup(rows_calc)
            )
        return
    if btn == 'помощь':
        clear_user_state(context)
        await cmd_help(update, context)
        return
    if btn == 'контакты':
        contacts = await ensure_contacts_loaded()
        if chat_id:
            if not contacts:
                await context.bot.send_message(chat_id=chat_id, text='❌ Контакты не найдены', reply_markup=reply_kb())
                return
            # Smart two-column keyboard for contacts
            buttons: List[InlineKeyboardButton] = []
            for idx, c in enumerate(contacts):
                label = c.get('Филиал/Офис') or 'Офис'
                buttons.append(InlineKeyboardButton(text=label, callback_data=f'contact|{idx}'))
            rows_kb: List[List[InlineKeyboardButton]] = []
            cur: List[InlineKeyboardButton] = []
            for b in buttons:
                if len(b.text) > 18:
                    if cur:
                        rows_kb.append(cur)
                        cur = []
                    rows_kb.append([b])
                else:
                    cur.append(b)
                    if len(cur) == 2:
                        rows_kb.append(cur)
                        cur = []
            if cur:
                rows_kb.append(cur)
            await context.bot.send_message(chat_id=chat_id, text='📞 <b>Выберите филиал/офис:</b>', parse_mode='HTML', reply_markup=InlineKeyboardMarkup(rows_kb))
        return


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    data = q.data or ''

    # --- Calculator callback flow ---
    if data.startswith('calc|'):
        parts = data.split('|')
        sub = parts[1] if len(parts) > 1 else ''
        if sub == 'mode':
            mode = parts[2] if len(parts) > 2 else 'area'
            # Show crop list filtered by mode
            await ensure_data_loaded()
            await q.message.edit_text(
                text='📋 Выберите культуру для расчёта',
                reply_markup=crops_page_keyboard_for_mode(mode, 0)
            )
            # Save mode only; culture will be saved later
            set_user_state(context, STATE_CALC_PESTICIDE_SELECT, calc_mode=mode)
            return
    if data.startswith('calccroppg|'):
        # Format: calccroppg|m:<mode>|<page>
        try:
            _, mode_part, page_str = data.split('|', 2)
            mode = mode_part[2:]
            page = int(page_str)
        except Exception:
            mode = 'area'
            page = 0
        await q.message.edit_text(
            text='📋 Выберите культуру для расчёта',
            reply_markup=crops_page_keyboard_for_mode(mode, page)
        )
        return
    if data.startswith('namepick|'):
        hh = data.split('h:',1)[1] if 'h:' in data else ''
        await ensure_data_loaded()
        rows = _DATA_CACHE['data']['rows']
        # find first name whose normalized hash matches
        def find_name() -> str:
            for r in rows:
                nm = get_val(r,'name')
                if nm and hash32(normalize_text(nm)) == hh:
                    return nm
            return ''
        nm = find_name()
        if not nm:
            await q.message.edit_text('❌ Не удалось определить препарат', reply_markup=None)
            return
        card = build_combined_product_card(nm, rows)
        await q.message.edit_text(text=card, parse_mode='HTML', reply_markup=None)
        return
    if data.startswith('calccrop|'):
        # Format: calccrop|m:<mode>|h:<hash>
        try:
            parts = data.split('|')
            mode = parts[1][2:]
            h = parts[2][2:]
        except Exception:
            mode = 'area'
            h = ''
        await ensure_data_loaded()
        rows = _DATA_CACHE['data']['rows']
        # Find crop by hash in filtered list
        crop_label = None
        for c in list_crops_for_mode(rows, mode):
            if hash32(crop_key_for_dedup(c)) == h:
                crop_label = c
                break
        if not crop_label:
            await q.message.edit_text(text='❌ Культура не найдена')
            return
        # Save culture and prompt for pesticide name with extra button
        set_user_state(context, STATE_CALC_PESTICIDE_SELECT, calc_mode=mode, culture=crop_label)
        await q.message.edit_text(
            text=(
                '✅ Культура выбрана\n\n'
                f'🌱 {crop_label}\n\n'
                '📝 Введите название препарата сообщением или выберите из списка.'
            ),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('📋 Все доступные препараты', callback_data=f'calc|allprods|m:{mode}|p:0')]])
        )
        return

    if data.startswith('croppg|'):
        page = int(data.split('|',1)[1])
        await q.message.edit_reply_markup(reply_markup=crops_page_keyboard(page))
        return

    # Remaining calculator callbacks
    if data.startswith('calc|allprods|'):
        # calc|allprods|m:<mode>|p:<page>
        try:
            _, _, mpart, ppart = data.split('|', 3)
            mode = mpart[2:]
            page = int(ppart[2:])
        except Exception:
            mode = context.user_data.get('calc_mode','area')
            page = 0
        culture = context.user_data.get('culture')
        await ensure_data_loaded()
        rows = _DATA_CACHE['data']['rows']
        if not culture:
            await q.message.edit_text('❌ Культура не выбрана')
            return
        await q.message.edit_text(
            text='📦 Выберите препарат из списка',
            reply_markup=all_products_keyboard(mode, culture, rows, page)
        )
        return

    if data.startswith('calc|pick|'):
        # calc|pick|m:<mode>|h:<hash>
        try:
            _, _, mpart, hpart = data.split('|', 3)
            mode = mpart[2:]
            hh = hpart[2:]
        except Exception:
            mode = context.user_data.get('calc_mode','area')
            hh = ''
        culture = context.user_data.get('culture')
        await ensure_data_loaded()
        rows = _DATA_CACHE['data']['rows']
        candidates = get_pesticides_for_culture_and_mode(rows, culture, mode)
        picked = None
        for r in candidates:
            nm = get_val(r, 'name')
            if nm and hash32(normalize_text(nm)) == hh:
                picked = r
                break
        if not picked:
            await q.message.edit_text('❌ Препарат не найден')
            return
        pesticide_name = get_val(picked, 'name')
        rate_str = get_val(picked, 'rate')
        comps = parse_rate_components(rate_str)
        if not comps:
            await q.message.edit_text('❌ Не удалось распознать норму расхода')
            return
        # Save in state and prompt next input
        if mode == 'tank':
            set_user_state(context, STATE_CALC_WATER_RATE_INPUT, calc_mode=mode, culture=culture, pesticide_name=pesticide_name, rate_str=rate_str, components=comps)
            await q.message.edit_text(
                text=f'🌱 Культура: <b>{culture}</b>\n✅ Препарат: <b>{pesticide_name}</b>\n💧 Норма: <b>{rate_str}</b>\n\n💦 Укажите норму воды (л/га):',
                reply_markup=None,
                parse_mode='HTML'
            )
        elif mode == 'area':
            set_user_state(context, STATE_CALC_AMOUNT_INPUT, calc_mode=mode, culture=culture, pesticide_name=pesticide_name, rate_str=rate_str, components=comps)
            await q.message.edit_text(
                text=f'🌱 Культура: <b>{culture}</b>\n✅ Препарат: <b>{pesticide_name}</b>\n💧 Норма: <b>{rate_str}</b>\n\n📏 Укажите площадь (га):',
                reply_markup=None,
                parse_mode='HTML'
            )
        else:  # seed
            set_user_state(context, STATE_CALC_AMOUNT_INPUT, calc_mode=mode, culture=culture, pesticide_name=pesticide_name, rate_str=rate_str, components=comps)
            await q.message.edit_text(
                text=f'🌱 Культура: <b>{culture}</b>\n✅ Препарат: <b>{pesticide_name}</b>\n💧 Норма: <b>{rate_str}</b>\n\n🌾 Укажите количество семян (т):',
                reply_markup=None,
                parse_mode='HTML'
            )
        return

    if data.startswith('calc|other_rate'):
        # Ask for custom rate number
        set_user_state(context, STATE_CALC_CUSTOM_RATE_INPUT)
        await q.message.edit_text('🔄 Введите свою норму (число). Она заменит первую норму препарата.', reply_markup=None)
        return
    if data.startswith('crop|'):
        await ensure_data_loaded()
        rows = _DATA_CACHE['data']['rows']
        parts = data.split('|')
        ch = parts[1][2:]
        crop = _CROPS_CACHE['map'].get(ch)
        kinds = unique_destroy_kinds_for_crop(rows, crop)
        kb_rows: List[List[InlineKeyboardButton]] = [[InlineKeyboardButton(text=k, callback_data=f'kind|ch:{ch}|k:{hash32(normalize_text(k))}')] for k in kinds]
        await q.message.edit_text(text=f'🌱 {crop}\nВыберите вид уничтожаемого объекта:', parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb_rows))
        return
    if data.startswith('kind|'):
        await ensure_data_loaded()
        rows = _DATA_CACHE['data']['rows']
        parts = data.split('|')
        ch = parts[1][3:]
        crop = _CROPS_CACHE['map'].get(ch)
        kind = next((p[2:] for p in parts if p.startswith('k:')), '')
        kinds_map = {}
        for k in unique_destroy_kinds_for_crop(rows, crop):
            kinds_map[hash32(normalize_text(k))] = k
        kind_label = kinds_map.get(kind,'')
        types = unique_types_for_crop_destroy(rows, crop, kind_label)
        kb_rows: List[List[InlineKeyboardButton]] = [[InlineKeyboardButton(text=short_type_label(t), callback_data=f'type|ch:{ch}|k:{kind}|t:{hash32(normalize_text(t))}')] for t in types]
        await q.message.edit_text(text=f'🌱 {crop}\n{kind_label}\nВыберите вид препарата:', parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb_rows))
        return
    if data.startswith('type|'):
        await ensure_data_loaded()
        rows = _DATA_CACHE['data']['rows']
        parts = data.split('|')
        ch = parts[1][3:]
        crop = _CROPS_CACHE['map'].get(ch)
        khash = next((p[2:] for p in parts if p.startswith('k:')), '')
        thash = next((p[2:] for p in parts if p.startswith('t:')), '')
        kinds_map = {}
        for k in unique_destroy_kinds_for_crop(rows, crop):
            kinds_map[hash32(normalize_text(k))] = k
        types_map = {}
        for t in unique_types_for_crop_destroy(rows, crop, kinds_map.get(khash,'')):
            types_map[hash32(normalize_text(t))] = t
        kind_label = kinds_map.get(khash,'')
        type_label = types_map.get(thash,'')
        filtered = filter_by_crop_type_destroy(rows, crop, type_label, kind_label)
        if not filtered:
            await q.message.edit_text(text='Не найдено', reply_markup=None)
            return
        chunks = []
        header = '› <i>'+type_label+'</i>\n'
        chunks = []
        for r in filtered[:10]:
            name = get_val(r,'name')
            ai = get_val(r,'ai')
            pests = get_val(r,'pests')
            rate = get_val(r,'rate')
            crops = get_val(r,'crops')
            line = []
            if name:
                line.append('🛡️ <b>'+name+'</b>')
            if ai:
                line.append('🧪 Д.в.: <b>'+ai+'</b>')
            if crops:
                line.append('🌱 Культура: <b>'+crops+'</b>')
            if pests:
                line.append('⚠️ Вредные объекты: <b>'+pests+'</b>')
            if rate:
                line.append('💧 Норма: <b>'+rate+'</b>')
            chunks.append('\n'.join(line))
        text_out = header + ('\n\n').join(chunks)
        await q.message.edit_text(text=text_out, parse_mode='HTML', reply_markup=None)
        return
    if data.startswith('contact|'):
        idx = int(data.split('|',1)[1]) if '|' in data else -1
        contacts = await ensure_contacts_loaded()
        if idx < 0 or idx >= len(contacts):
            await q.message.edit_text(text='❌ Контакт не найден', parse_mode='HTML')
            return
        c = contacts[idx]
        filial = c.get('Филиал/Офис','')
        address = c.get('Адрес','')
        phones: List[str] = []
        for k, v in c.items():
            if 'телефон' in k.lower():
                for part in str(v).split(','):
                    p = part.strip()
                    if p:
                        phones.append(p)
        msg = '📞 <b>'+filial+'</b>\n\n'
        if address:
            msg += '📍 <i>Адрес:</i>\n' + address + '\n\n'
        if phones:
            msg += '☎️ <i>Телефоны:</i>\n' + '\n'.join('<code>'+ph+'</code>' for ph in phones)
        for k, v in c.items():
            kl = k.lower()
            if k not in ('Филиал/Офис','Адрес') and 'телефон' not in kl:
                vv = str(v).strip()
                if vv:
                    msg += '\n' + k + ': ' + vv
        await q.message.edit_text(text=msg, parse_mode='HTML')
        return
    await q.message.reply_text(f'CB: {data}')


def main():
    logger = logging.getLogger(__name__)
    
    logger.info("Запуск Telegram бота...")
    
    token = os.getenv('TELEGRAM_TOKEN')
    if not token:
        logger.error("TELEGRAM_TOKEN не установлен!")
        raise RuntimeError('TELEGRAM_TOKEN not set')
    
    logger.info(f"Токен найден: {token[:10]}...")
    
    try:
        app = ApplicationBuilder().token(token).build()
        logger.info("ApplicationBuilder создан успешно")
    except Exception as e:
        logger.error(f"Ошибка создания ApplicationBuilder: {e}")
        raise
    
    # Добавляем обработчики
    app.add_handler(CommandHandler('start', cmd_start))
    app.add_handler(CommandHandler('menu', cmd_menu))
    app.add_handler(CommandHandler('help', cmd_help))
    app.add_handler(CommandHandler('setcommands', cmd_setcommands))
    app.add_handler(CommandHandler('reload', cmd_reload))
    app.add_handler(CommandHandler('dbg_on', cmd_dbg_on))
    app.add_handler(CommandHandler('dbg_off', cmd_dbg_off))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), on_message))
    app.add_handler(CallbackQueryHandler(on_callback))
    logger.info("Все обработчики добавлены")

    use_webhook = os.getenv('USE_WEBHOOK', '0') == '1'
    logger.info(f"Режим работы: {'webhook' if use_webhook else 'polling'}")
    
    if use_webhook:
        public_url = os.getenv('RENDER_EXTERNAL_URL') or os.getenv('PUBLIC_URL')
        port = int(os.getenv('PORT', '10000'))
        if not public_url:
            logger.error("PUBLIC_URL или RENDER_EXTERNAL_URL не установлены для webhook")
            raise RuntimeError('PUBLIC_URL or RENDER_EXTERNAL_URL not set for webhook')
        
        path = f"/webhook/{token}"
        webhook_url = public_url + path
        logger.info(f"Запуск webhook на порту {port}, URL: {webhook_url}")
        
        try:
            app.run_webhook(
                listen='0.0.0.0',
                port=port,
                webhook_url=webhook_url,
                url_path=path,
                allowed_updates=Update.ALL_TYPES
            )
        except Exception as e:
            logger.error(f"Ошибка запуска webhook: {e}")
            raise
    else:
        logger.info("Запуск polling режима...")
        try:
            # Сначала удаляем webhook если он был установлен
            logger.info("Удаляем старый webhook (если есть)...")
            import asyncio
            async def delete_webhook():
                try:
                    await app.bot.delete_webhook(drop_pending_updates=True)
                    logger.info("Webhook удален")
                except Exception as e:
                    logger.warning(f"Не удалось удалить webhook: {e}")
            
            asyncio.run(delete_webhook())
            
            logger.info("Начинаем polling...")
            # Исправление для Python 3.13+: создаем event loop явно

            try:

                loop = asyncio.get_event_loop()

            except RuntimeError:

                loop = asyncio.new_event_loop()

                asyncio.set_event_loop(loop)

            

            app.run_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True
            )
        except Exception as e:
            logger.error(f"Ошибка запуска polling: {e}")
            raise


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        raise

