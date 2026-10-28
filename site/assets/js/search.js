/* Canal Rust — dependency-free site search over both languages */
(function () {
  'use strict';

  var doc = document;
  var button = doc.querySelector('.search-toggle');
  if (!button) return;

  var isEn = /^en/i.test(doc.documentElement.lang || '');
  var copy = isEn ? {
    label: 'Search',
    placeholder: 'Search pages, docs, keywords…',
    initial: 'Type to search across the site.',
    loading: 'Loading search index…',
    error: 'Could not load the search index.',
    none: 'No results for',
    found: function (n) { return n + (n === 1 ? ' result for' : ' results for'); },
    hint: '↑↓ select · Enter open · Esc close',
    close: 'Close search',
    zh: '中文',
    en: 'EN'
  } : {
    label: '搜索',
    placeholder: '搜索页面、文档、关键字…',
    initial: '输入关键词开始搜索。',
    loading: '正在加载搜索索引…',
    error: '搜索索引加载失败。',
    none: '没有找到结果：',
    found: function (n) { return '找到 ' + n + ' 个结果：'; },
    hint: '↑↓ 选择 · Enter 打开 · Esc 关闭',
    close: '关闭搜索',
    zh: '中文',
    en: 'EN'
  };

  var dialog = doc.createElement('dialog');
  dialog.className = 'search-dialog';
  dialog.setAttribute('aria-label', isEn ? 'Site search' : '站内搜索');
  dialog.innerHTML =
    '<div class="search-shell">' +
      '<div class="search-head">' +
        '<svg viewBox="0 0 24 24" width="19" height="19" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>' +
        '<input class="search-input" type="search" autocomplete="off" spellcheck="false" aria-controls="search-results">' +
        '<button class="search-close" type="button" aria-label="' + copy.close + '">×</button>' +
      '</div>' +
      '<div class="search-status" aria-live="polite"></div>' +
      '<ul class="search-results" id="search-results"></ul>' +
      '<div class="search-hint">' + copy.hint + '</div>' +
    '</div>';
  doc.body.appendChild(dialog);

  var input = dialog.querySelector('.search-input');
  var closeButton = dialog.querySelector('.search-close');
  var status = dialog.querySelector('.search-status');
  var resultsList = dialog.querySelector('.search-results');
  var docs = null;
  var loading = null;
  var loadFailed = false;
  var debounceTimer = 0;
  var selected = -1;

  input.placeholder = copy.placeholder;
  button.setAttribute('aria-label', copy.label);

  function escapeHtml(value) {
    return String(value).replace(/[&<>"']/g, function (char) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char];
    });
  }

  function escapeRegExp(value) {
    return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  function highlight(raw, terms) {
    var cleaned = terms.filter(Boolean).sort(function (a, b) { return b.length - a.length; });
    if (!cleaned.length) return escapeHtml(raw);
    var regex = new RegExp('(' + cleaned.map(escapeRegExp).join('|') + ')', 'gi');
    return String(raw).split(regex).map(function (part, i) {
      return i % 2 ? '<mark>' + escapeHtml(part) + '</mark>' : escapeHtml(part);
    }).join('');
  }

  function snippetFor(docItem, terms) {
    var text = (docItem.text || '').replace(/\s+/g, ' ').trim();
    var lower = text.toLowerCase();
    var index = -1;
    terms.forEach(function (term) {
      var at = lower.indexOf(term.toLowerCase());
      if (at >= 0 && (index < 0 || at < index)) index = at;
    });
    if (index < 0) {
      text = docItem.description || docItem.title;
    } else {
      var start = Math.max(0, index - 58);
      var end = Math.min(text.length, index + 118);
      text = (start > 0 ? '…' : '') + text.slice(start, end) + (end < text.length ? '…' : '');
    }
    return highlight(text, terms);
  }

  function scoreDoc(docItem, terms, requireAll) {
    var title = (docItem.title || '').toLowerCase();
    var headings = (docItem.headings || []).join(' ').toLowerCase();
    var description = (docItem.description || '').toLowerCase();
    var text = (docItem.text || '').toLowerCase();
    var score = 0;
    var matched = 0;
    for (var i = 0; i < terms.length; i += 1) {
      var term = terms[i];
      var inTitle = title.indexOf(term) >= 0;
      var inHeadings = headings.indexOf(term) >= 0;
      var inDescription = description.indexOf(term) >= 0;
      var inText = text.indexOf(term) >= 0;
      if (!(inTitle || inHeadings || inDescription || inText)) {
        if (requireAll) return -1;
        continue;
      }
      matched += 1;
      if (inTitle) score += 12;
      if (inHeadings) score += 6;
      if (inDescription) score += 4;
      if (inText) score += 1;
    }
    if (!matched) return -1;
    var query = terms.join(' ');
    if (title.indexOf(query) >= 0) score += 16;
    if ((description + ' ' + headings).indexOf(query) >= 0) score += 6;
    return score;
  }

  function search(query) {
    if (!docs) return [];
    var q = query.toLowerCase().trim();
    if (!q) return [];
    var terms = q.split(/\s+/).filter(Boolean);
    var pool = docs.map(function (docItem) {
      return { doc: docItem, score: scoreDoc(docItem, terms, true) };
    }).filter(function (item) { return item.score >= 0; });
    if (!pool.length) {
      pool = docs.map(function (docItem) {
        return { doc: docItem, score: scoreDoc(docItem, terms, false) };
      }).filter(function (item) { return item.score >= 0; });
    }
    pool.sort(function (a, b) {
      return b.score - a.score || a.doc.title.localeCompare(b.doc.title);
    });
    return pool.slice(0, 12).map(function (item) {
      return { doc: item.doc, score: item.score, snippet: snippetFor(item.doc, terms) };
    });
  }

  function setStatus(text) {
    status.textContent = text || '';
  }

  function renderResults(results, query) {
    selected = -1;
    if (!query.trim()) {
      resultsList.innerHTML = '';
      setStatus(copy.initial);
      return;
    }
    if (!results.length) {
      resultsList.innerHTML = '';
      setStatus(copy.none + ' “' + query.trim() + '”');
      return;
    }
    setStatus(copy.found(results.length) + ' “' + query.trim() + '”');
    resultsList.innerHTML = results.map(function (item, index) {
      var lang = item.doc.lang === 'en' ? copy.en : copy.zh;
      return '<li><a class="search-result" href="' + escapeHtml(item.doc.url) + '" data-result="' + index + '">' +
        '<span class="search-result-head"><span class="search-result-title">' + highlight(item.doc.title, query.toLowerCase().split(/\s+/)) + '</span>' +
        '<span class="search-result-lang">' + lang + '</span></span>' +
        '<span class="search-result-snippet">' + item.snippet + '</span>' +
        '</a></li>';
    }).join('');
  }

  function runSearch() {
    renderResults(search(input.value), input.value);
  }

  function ensureIndex() {
    if (docs || loading) return loading;
    setStatus(copy.loading);
    loading = fetch('search-index.json', { cache: 'no-cache' })
      .then(function (response) {
        if (!response.ok) throw new Error('search index: ' + response.status);
        return response.json();
      })
      .then(function (data) {
        docs = data.docs || [];
        loading = null;
        if (input.value.trim()) runSearch();
        else setStatus(copy.initial);
      })
      .catch(function () {
        loadFailed = true;
        loading = null;
        setStatus(copy.error);
      });
    return loading;
  }

  function open() {
    doc.body.classList.add('search-open');
    if (!dialog.open) dialog.showModal();
    ensureIndex();
    window.setTimeout(function () {
      input.focus();
      input.select();
    }, 0);
  }

  function close() {
    if (dialog.open) dialog.close();
  }

  function setSelected(index) {
    var items = Array.prototype.slice.call(resultsList.querySelectorAll('.search-result'));
    if (!items.length) return;
    selected = Math.max(0, Math.min(items.length - 1, index));
    items.forEach(function (item, i) {
      item.classList.toggle('is-selected', i === selected);
    });
    items[selected].scrollIntoView({ block: 'nearest' });
  }

  button.addEventListener('click', function (event) {
    event.preventDefault();
    open();
  });
  closeButton.addEventListener('click', close);
  dialog.addEventListener('click', function (event) {
    if (event.target === dialog) close();
  });
  dialog.addEventListener('close', function () {
    doc.body.classList.remove('search-open');
    button.focus();
  });
  dialog.addEventListener('cancel', function () {
    doc.body.classList.remove('search-open');
  });

  input.addEventListener('input', function () {
    window.clearTimeout(debounceTimer);
    debounceTimer = window.setTimeout(function () {
      if (docs) runSearch();
    }, 90);
  });
  input.addEventListener('keydown', function (event) {
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      setSelected(selected < 0 ? 0 : selected + 1);
    } else if (event.key === 'ArrowUp') {
      event.preventDefault();
      setSelected(selected <= 0 ? 0 : selected - 1);
    } else if (event.key === 'Enter') {
      var items = resultsList.querySelectorAll('.search-result');
      if (!items.length) return;
      event.preventDefault();
      (items[selected] || items[0]).click();
    }
  });
  resultsList.addEventListener('click', function (event) {
    if (event.target.closest('a')) close();
  });

  doc.addEventListener('keydown', function (event) {
    var tag = (event.target.tagName || '').toLowerCase();
    var typing = tag === 'input' || tag === 'textarea' || tag === 'select' || event.target.isContentEditable;
    var shortcut = (event.key === '/' && !typing) ||
      ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k');
    if (!shortcut) return;
    event.preventDefault();
    open();
  });
})();
