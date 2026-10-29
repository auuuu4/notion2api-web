function escHtml(s) {
  if (s == null) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function setText(id, val) {
  var el = document.getElementById(id);
  if (el) el.textContent = val;
}
function fmtTime(ts) {
  if (!ts) return '—';
  var d = new Date(ts);
  if (isNaN(d.getTime())) return '—';
  return d.toLocaleTimeString('zh-CN', { hour12: false });
}
function fmtDuration(seconds) {
  if (seconds == null || seconds < 0) return '—';
  var d = Math.floor(seconds / 86400);
  var h = Math.floor((seconds % 86400) / 3600);
  var m = Math.floor((seconds % 3600) / 60);
  var s = Math.floor(seconds % 60);
  var parts = [];
  if (d > 0) parts.push(d + '天');
  if (h > 0) parts.push(h + '小时');
  if (m > 0) parts.push(m + '分钟');
  if (parts.length === 0 || s > 0) parts.push(s + '秒');
  return parts.join(' ');
}
function showToast(message, type) {
  var container = document.getElementById('toastContainer');
  if (!container) return;
  var toasts = container.querySelectorAll('.toast');
  while (toasts.length >= 5) { toasts[0].remove(); toasts = container.querySelectorAll('.toast'); }
  var toast = document.createElement('div');
  toast.className = 'toast ' + (type || 'info');
  toast.textContent = message;
  container.appendChild(toast);
  setTimeout(function() { if (toast.parentNode) toast.remove(); }, 3500);
}
async function apiFetch(url, options) {
  try {
    var resp = await fetch(url, options || {});
    if (!resp.ok) {
      var text = await resp.text();
      throw new Error('[' + resp.status + '] ' + text.slice(0, 200));
    }
    return await resp.json();
  } catch (e) {
    showToast(e.message, 'error');
    throw e;
  }
}

function statusLabel(status) {
  var map = { active: '正常', quota_exceeded: '额度耗尽', disabled: '已禁用',
              success: '成功', error: '失败', pending: '等待中', deleted: '已删除' };
  return map[status] || status || '—';
}

function sourceLabel(account) {
  if (!account) return '—';
  return account.auto_registered ? '自动' : '手动';
}

function fmtConfigValue(value) {
  if (value === null || value === undefined || value === '') return '—';
  if (Array.isArray(value)) return value.length ? value.join(', ') : '（空）';
  if (typeof value === 'object') {
    var parts = [];
    Object.keys(value).forEach(function(k) { parts.push(k + '=' + value[k]); });
    return parts.length ? parts.join(' · ') : '（空）';
  }
  if (typeof value === 'boolean') return value ? '开启' : '关闭';
  return String(value);
}

function openExternal(url) { window.open(url, '_blank'); }
