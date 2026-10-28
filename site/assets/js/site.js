/* Canal Rust — static site interactions (no dependencies) */
(function () {
  'use strict';

  var reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  var doc = document;
  var isEn = /^en/i.test(doc.documentElement.lang || '');
  var root = doc.documentElement;

  /* Mobile navigation */
  var toggle = doc.querySelector('.nav-toggle');
  var nav = doc.getElementById('site-nav');
  if (toggle && nav) {
    var setNav = function (open) {
      doc.body.classList.toggle('nav-open', open);
      toggle.setAttribute('aria-expanded', String(open));
      toggle.setAttribute('aria-label', open ? (isEn ? 'Close navigation menu' : '关闭导航菜单') : (isEn ? 'Open navigation menu' : '打开导航菜单'));
    };
    toggle.addEventListener('click', function () {
      setNav(!doc.body.classList.contains('nav-open'));
    });
    nav.addEventListener('click', function (event) {
      if (event.target.closest('a')) setNav(false);
    });
    doc.addEventListener('keydown', function (event) {
      if (event.key === 'Escape') setNav(false);
    });
    window.addEventListener('resize', function () {
      if (window.innerWidth > 900) setNav(false);
    }, { passive: true });
  }

  /* Back-to-top pet: visibility + scroll progress ring */
  var backtop = doc.getElementById('backtop');
  var header = doc.querySelector('.site-header');
  var tocLinks = Array.prototype.slice.call(doc.querySelectorAll('.doc-toc a[href^="#"]'));
  var tocSections = tocLinks.map(function (link) {
    return doc.getElementById(link.getAttribute('href').slice(1));
  });

  function updateToc() {
    if (!tocLinks.length) return;
    var current = -1;
    for (var i = 0; i < tocSections.length; i++) {
      if (tocSections[i] && tocSections[i].getBoundingClientRect().top <= 140) current = i;
    }
    tocLinks.forEach(function (link, i) {
      link.classList.toggle('is-active', i === current);
    });
  }

  var ticking = false;
  function onScroll() {
    if (ticking) return;
    ticking = true;
    window.requestAnimationFrame(function () {
      var y = window.scrollY || window.pageYOffset;
      if (header) header.classList.toggle('is-scrolled', y > 8);
      if (backtop) {
        var max = doc.documentElement.scrollHeight - window.innerHeight;
        var pct = max > 0 ? Math.min(100, Math.max(0, (y / max) * 100)) : 0;
        backtop.style.setProperty('--scroll', pct.toFixed(1));
        backtop.classList.toggle('is-visible', y > 480);
      }
      updateToc();
      ticking = false;
    });
  }
  window.addEventListener('scroll', onScroll, { passive: true });
  window.addEventListener('resize', onScroll, { passive: true });
  onScroll();

  if (backtop) {
    backtop.addEventListener('click', function () {
      window.scrollTo({ top: 0, behavior: reduceMotion ? 'auto' : 'smooth' });
    });
  }

  /* Scroll reveal */
  var revealItems = Array.prototype.slice.call(doc.querySelectorAll('.reveal'));
  if (revealItems.length) {
    if (reduceMotion || !('IntersectionObserver' in window)) {
      revealItems.forEach(function (el) { el.classList.add('is-visible'); });
    } else {
      var revealObserver = new IntersectionObserver(function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            entry.target.classList.add('is-visible');
            revealObserver.unobserve(entry.target);
          }
        });
      }, { rootMargin: '0px 0px -8% 0px', threshold: 0.08 });
      revealItems.forEach(function (el) { revealObserver.observe(el); });
    }
  }

  /* Copy buttons on code blocks */
  doc.querySelectorAll('.code-copy').forEach(function (button) {
    button.addEventListener('click', function () {
      var block = button.closest('.code-block');
      var code = block && block.querySelector('pre');
      if (!code) return;
      var text = code.innerText;
      var done = function (ok) {
        button.textContent = ok ? (isEn ? 'Copied' : '已复制') : (isEn ? 'Copy failed' : '复制失败');
        window.setTimeout(function () { button.textContent = isEn ? 'Copy' : '复制'; }, 1600);
      };
      if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(text).then(function () { done(true); }, function () { done(false); });
      } else {
        var area = doc.createElement('textarea');
        area.value = text;
        area.setAttribute('readonly', '');
        area.style.cssText = 'position:fixed;left:-9999px;top:0';
        doc.body.appendChild(area);
        area.select();
        var ok = false;
        try { ok = doc.execCommand('copy'); } catch (err) { ok = false; }
        doc.body.removeChild(area);
        done(ok);
      }
    });
  });

  /* Seamless pet marquee: duplicate once, then start the animation */
  doc.querySelectorAll('.pet-track').forEach(function (track) {
    if (reduceMotion) return;
    track.innerHTML += track.innerHTML;
    track.classList.add('is-running');
  });

  /* Image lightbox: click a content image to inspect and zoom it */
  (function initLightbox() {
    if (typeof HTMLDialogElement === 'undefined' || !('showModal' in HTMLDialogElement.prototype)) return;

    var images = [];
    doc.querySelectorAll('main img').forEach(function (img) {
      if (img.closest('.pet-marquee, .hero-pet-chip, .stamp, .release-pet, .pet-note')) return;
      images.push(img);
    });
    if (!images.length) return;

    var dialog = doc.createElement('dialog');
    dialog.className = 'lightbox';
    dialog.setAttribute('aria-label', isEn ? 'Image viewer' : '图片查看器');
    dialog.innerHTML =
      '<div class="lightbox-shell">' +
        '<div class="lightbox-toolbar">' +
          '<button type="button" data-lb="out" aria-label="' + (isEn ? 'Zoom out' : '缩小') + '">−</button>' +
          '<button type="button" class="lightbox-scale" data-lb="reset" aria-label="' + (isEn ? 'Reset zoom' : '重置缩放') + '">100%</button>' +
          '<button type="button" data-lb="in" aria-label="' + (isEn ? 'Zoom in' : '放大') + '">+</button>' +
          '<button type="button" class="lightbox-close" data-lb="close" aria-label="' + (isEn ? 'Close' : '关闭') + '">×</button>' +
        '</div>' +
        '<div class="lightbox-viewport"><img class="lightbox-image" alt="" draggable="false"></div>' +
        '<p class="lightbox-caption"></p>' +
        '<p class="lightbox-hint">' + (isEn ? 'Scroll / double-click to zoom · Drag to pan · Esc to close' : '滚轮 / 双击放大 · 拖动平移 · Esc 关闭') + '</p>' +
      '</div>';
    doc.body.appendChild(dialog);

    var viewport = dialog.querySelector('.lightbox-viewport');
    var image = dialog.querySelector('.lightbox-image');
    var caption = dialog.querySelector('.lightbox-caption');
    var scaleButton = dialog.querySelector('.lightbox-scale');
    var closeButton = dialog.querySelector('.lightbox-close');
    var scale = 1;
    var x = 0;
    var y = 0;
    var lastFocus = null;
    var dragging = false;
    var dragStartX = 0;
    var dragStartY = 0;
    var startX = 0;
    var startY = 0;
    var pointerId = null;
    var MIN = 1;
    var MAX = 8;

    function clamp(value, min, max) {
      return Math.min(max, Math.max(min, value));
    }

    function render() {
      image.style.transform = 'translate(' + x + 'px,' + y + 'px) scale(' + scale + ')';
      scaleButton.textContent = Math.round(scale * 100) + '%';
      viewport.classList.toggle('is-zoomed', scale > 1.001);
    }

    function clampPan() {
      if (scale <= 1.001) {
        x = 0;
        y = 0;
        return;
      }
      var maxX = viewport.clientWidth * (scale - 1) / 2;
      var maxY = viewport.clientHeight * (scale - 1) / 2;
      x = clamp(x, -maxX, maxX);
      y = clamp(y, -maxY, maxY);
    }

    function reset() {
      scale = 1;
      x = 0;
      y = 0;
      render();
    }

    function zoomTo(next, cx, cy) {
      var target = clamp(next, MIN, MAX);
      if (cx === undefined) { cx = 0; cy = 0; }
      var ratio = target / scale;
      x = cx - (cx - x) * ratio;
      y = cy - (cy - y) * ratio;
      scale = target;
      if (scale <= 1.001) {
        scale = 1;
        x = 0;
        y = 0;
      }
      clampPan();
      render();
    }

    function open(img) {
      lastFocus = img;
      var label = img.alt || '';
      if (!label) {
        var figure = img.closest('figure');
        var figcaption = figure && figure.querySelector('figcaption');
        if (figcaption) label = figcaption.textContent.trim();
      }
      image.src = img.currentSrc || img.src;
      image.alt = label;
      caption.textContent = label;
      reset();
      doc.body.classList.add('lightbox-open');
      if (!dialog.open) dialog.showModal();
      closeButton.focus();
    }

    function close() {
      if (dialog.open) dialog.close();
    }

    images.forEach(function (img) {
      img.classList.add('zoomable');
      img.tabIndex = 0;
      img.setAttribute('role', 'button');
      img.setAttribute('aria-label', (isEn ? 'View larger image' : '查看大图') + (img.alt ? (isEn ? ': ' : '：') + img.alt : ''));
      img.addEventListener('click', function () { open(img); });
      img.addEventListener('keydown', function (event) {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          open(img);
        }
      });
    });

    dialog.addEventListener('click', function (event) {
      if (event.target === dialog) close();
    });
    dialog.addEventListener('close', function () {
      doc.body.classList.remove('lightbox-open');
      image.removeAttribute('src');
      if (lastFocus) lastFocus.focus();
    });
    dialog.querySelectorAll('[data-lb]').forEach(function (button) {
      button.addEventListener('click', function () {
        var action = button.getAttribute('data-lb');
        if (action === 'close') close();
        else if (action === 'in') zoomTo(scale * 1.3);
        else if (action === 'out') zoomTo(scale / 1.3);
        else if (action === 'reset') reset();
      });
    });

    viewport.addEventListener('wheel', function (event) {
      if (!dialog.open) return;
      event.preventDefault();
      var rect = viewport.getBoundingClientRect();
      var cx = event.clientX - rect.left - rect.width / 2;
      var cy = event.clientY - rect.top - rect.height / 2;
      zoomTo(scale * (event.deltaY < 0 ? 1.18 : 1 / 1.18), cx, cy);
    }, { passive: false });

    viewport.addEventListener('dblclick', function (event) {
      event.preventDefault();
      if (scale > 1.01) reset();
      else zoomTo(2.5);
    });

    viewport.addEventListener('pointerdown', function (event) {
      if (scale <= 1.001 || event.button !== 0) return;
      dragging = true;
      pointerId = event.pointerId;
      dragStartX = event.clientX;
      dragStartY = event.clientY;
      startX = x;
      startY = y;
      viewport.setPointerCapture(pointerId);
      viewport.classList.add('is-dragging');
    });
    viewport.addEventListener('pointermove', function (event) {
      if (!dragging) return;
      x = startX + (event.clientX - dragStartX);
      y = startY + (event.clientY - dragStartY);
      clampPan();
      render();
    });
    function endDrag() {
      if (!dragging) return;
      dragging = false;
      viewport.classList.remove('is-dragging');
      if (pointerId !== null) {
        try { viewport.releasePointerCapture(pointerId); } catch (err) {}
        pointerId = null;
      }
    }
    viewport.addEventListener('pointerup', endDrag);
    viewport.addEventListener('pointercancel', endDrag);
  })();

  /* Prefetch internal pages on intent, so navigation feels instant */
  if ('requestAnimationFrame' in window) {
    var prefetched = {};
    doc.querySelectorAll('.site-nav a[href$=".html"], .footer-nav a[href$=".html"]').forEach(function (link) {
      var run = function () {
        var href = link.getAttribute('href');
        if (!href || prefetched[href]) return;
        prefetched[href] = true;
        var hint = doc.createElement('link');
        hint.rel = 'prefetch';
        hint.href = href;
        doc.head.appendChild(hint);
      };
      link.addEventListener('mouseenter', run, { once: true });
      link.addEventListener('focus', run, { once: true });
    });
  }

  /* Mark JS as available for enhancement-only styles */
  root.classList.add('js-ready');
})();
