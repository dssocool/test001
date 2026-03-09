(function () {
  'use strict';
  window.App = window.App || {};

  function initDomainsPage() {
    var searchEl = document.getElementById('domains-search');
    var gridEl = document.getElementById('domains-grid');
    if (searchEl && gridEl) {
      searchEl.addEventListener('input', function () {
        var q = (searchEl.value || '').trim().toLowerCase();
        var cards = gridEl.querySelectorAll('.domain-card');
        cards.forEach(function (card) {
          var name = (card.getAttribute('data-domain-name') || '').toLowerCase();
          var desc = (card.getAttribute('data-domain-desc') || '').toLowerCase();
          var show = !q || name.indexOf(q) !== -1 || desc.indexOf(q) !== -1;
          card.style.display = show ? '' : 'none';
        });
      });
    }

    document.querySelectorAll('.domain-card-menu-btn').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.stopPropagation();
        var wrap = btn.closest('.domain-card-menu-wrap');
        var dropdown = wrap && wrap.querySelector('.domain-card-dropdown');
        var isOpen = dropdown && !dropdown.hidden;
        document.querySelectorAll('.domain-card-dropdown').forEach(function (d) { d.hidden = true; });
        document.querySelectorAll('.domain-card-menu-btn').forEach(function (b) { b.setAttribute('aria-expanded', 'false'); });
        if (dropdown) {
          if (isOpen) {
            dropdown.hidden = true;
            btn.setAttribute('aria-expanded', 'false');
          } else {
            dropdown.hidden = false;
            btn.setAttribute('aria-expanded', 'true');
          }
        }
      });
    });

    document.addEventListener('click', function () {
      document.querySelectorAll('.domain-card-dropdown').forEach(function (d) { d.hidden = true; });
      document.querySelectorAll('.domain-card-menu-btn').forEach(function (b) { b.setAttribute('aria-expanded', 'false'); });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initDomainsPage);
  } else {
    initDomainsPage();
  }
})();
