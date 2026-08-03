import React from 'react';

const GlobalStyles = () => (
  <style>{`
    /* ----- Modern CSS Reset (based on modern-normalize) ----- */
    *,
    *::before,
    *::after {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }
    html {
      height: 100%;
      -moz-text-size-adjust: none;
      -webkit-text-size-adjust: none;
      text-size-adjust: none;
    }
    body {
      height: 100%;
      min-height: 100vh;
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
      -moz-osx-font-smoothing: grayscale;
      font-family: var(--font-sans);
      background: var(--bg-primary);
      color: var(--text-primary);
      font-size: var(--text-base);
    }
    h1, h2, h3, h4, button, input, label {
      line-height: 1.1;
    }
    h1, h2, h3, h4 {
      text-wrap: balance;
    }
    img, picture, video, canvas, svg {
      display: block;
      max-width: 100%;
    }
    input, button, textarea, select {
      font: inherit;
    }
    p, h1, h2, h3, h4, h5, h6 {
      overflow-wrap: break-word;
    }
    #root {
      isolation: isolate;
      height: 100%;
    }

    /* ----- Fonts & Variables ----- */
    @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=DM+Sans:opsz,wght@9..40,300;9..40,400;9..40,500;9..40,600;9..40,700&display=swap');

    :root {
      /* Colors - Core - Dark Theme (default) */
      --bg-primary: #0a0a0c;
      --bg-secondary: #121214;
      --bg-card: #1a1a1e;
      --bg-elevated: #22222a;       /* New: for hover states, dropdowns */
      --bg-input: #1e1e24;
      --bg-overlay: rgba(0, 0, 0, 0.5);
      --bg-overlay-light: rgba(255, 255, 255, 0.05);

      /* Borders */
      --border-subtle: #2a2a30;
      --border-strong: #3a3a44;
      --border-accent: var(--accent);

      /* Text */
      --text-primary: #f0f0f8;
      --text-secondary: #b0b0c0;
      --text-muted: #70707c;
      --text-inverse: #0a0a0c;

      /* Accent & Status */
      --accent: #6366f1;
      --accent-gradient: linear-gradient(135deg, #6366f1, #a855f7);
      --accent-light: rgba(99, 102, 241, 0.15);
      --accent-overlay-subtle: rgba(99, 102, 241, 0.25);
      --text-on-accent: #ffffff;
      --success: #10b981;
      --success-light: rgba(16, 185, 129, 0.15);
      --success-overlay-subtle: rgba(16, 185, 129, 0.18);
      --success-border: rgba(16, 185, 129, 0.3);
      --success-text: #34d399;
      --success-text-light: #6ee7b7;
      --warning: #f59e0b;
      --warning-light: rgba(245, 158, 11, 0.15);
      --warning-border: rgba(245, 158, 11, 0.3);
      --warning-text: #fcd34d;
      --danger: #ef4444;
      --danger-light: rgba(239, 68, 68, 0.15);
      --danger-border: rgba(239, 68, 68, 0.3);
      --danger-text: #fca5a5;
      --info: #3b82f6;
      --info-light: rgba(59, 130, 246, 0.15);

      /* Additional Colors for UI Components */
      --color-blue: #60a5fa;
      --color-blue-light: rgba(96, 165, 250, 0.15);
      --color-cyan: #2dd4bf;
      --color-cyan-light: rgba(45, 212, 191, 0.15);
      --color-purple: #a78bfa;
      --color-purple-light: rgba(167, 139, 250, 0.15);
      --color-red: #f87171;
      --color-red-light: rgba(248, 113, 113, 0.15);
      --color-pink: #f472b6;
      --color-pink-light: rgba(244, 114, 182, 0.15);

      /* Subtle background overlays */
      --accent-subtle: rgba(99, 102, 241, 0.05);
      --success-subtle: rgba(16, 185, 129, 0.05);
      --success-bg: rgba(16, 185, 129, 0.08);
      --success-border: rgba(16, 185, 129, 0.2);
      --danger-bg: rgba(239, 68, 68, 0.08);
      --danger-border: rgba(239, 68, 68, 0.2);

      /* Additional shadow effects */
      --shadow-accent: 0 4px 12px rgba(99, 102, 241, 0.3);

      /* Toast backgrounds */
      --toast-success-bg: #064e3b;
      --toast-error-bg: #7f1d1d;
      --toast-info-bg: #1e3a5f;

      /* Badge borders */
      --success-border: rgba(16, 185, 129, 0.3);
      --warning-border: rgba(245, 158, 11, 0.3);
      --gray-border: rgba(107, 114, 128, 0.3);
      --gray-color: #6b7280;

      /* Text colors for light backgrounds */
      --text-on-light: #000000;

      /* Additional shadow effects */
      --shadow-accent: 0 4px 12px rgba(99, 102, 241, 0.3);
      --shadow-accent-lg: 0 8px 20px rgba(99, 102, 241, 0.4);

      /* Danger button variants */
      --danger-border-light: rgba(239, 68, 68, 0.3);
      --danger-bg-hover: rgba(239, 68, 68, 0.25);

      /* Skeleton gradient */
      --skeleton-gradient: linear-gradient(90deg, #1e1e24 25%, #2a2a34 50%, #1e1e24 75%);

      /* Connect Store Panel */
      --panel-gradient: linear-gradient(180deg, rgba(28,28,34,0.92), rgba(16,16,20,0.96));
      --panel-overlay-light: rgba(255, 255, 255, 0.08);
      --panel-overlay-subtle: rgba(255, 255, 255, 0.05);
      --panel-overlay-very-subtle: rgba(255, 255, 255, 0.03);
      --panel-overlay-minimal: rgba(255, 255, 255, 0.04);

      /* Success accents */
      --success-gradient: radial-gradient(circle, rgba(16,185,129,0.32), rgba(16,185,129,0));
      --success-text: #34d399;
      --success-text-light: #6ee7b7;
      --success-icon-gradient: linear-gradient(135deg, rgba(16,185,129,0.18), rgba(99,102,241,0.18));
      --success-icon-border: rgba(16,185,129,0.28);
      --success-border-light: rgba(16,185,129, 0.2);

      /* Error messages */
      --error-bg-light: rgba(239, 68, 68, 0.08);
      --error-border-light: rgba(239, 68, 68, 0.22);
      --error-text-light: #fca5a5;
      --error-border: rgba(239, 68, 68, 0.2);

      /* Info box */
      --info-box-bg: rgba(99, 102, 241, 0.08);
      --info-box-border: rgba(99, 102, 241, 0.18);
      --info-box-text: #ddd6fe;
      --info-box-label: #c4b5fd;

      /* Accent UI elements */
      --accent-text: #818cf8;
      --accent-gradient-bg: rgba(99, 102, 241, 0.28);
      --accent-button-shadow: 0 18px 40px rgba(99, 102, 241, 0.18);

      /* Purple accents */
      --purple-gradient-bg: rgba(168, 85, 247, 0.16);

      /* Login page */
      --login-card-dark: rgba(24,24,30,0.97);
      --login-card-darker: rgba(14,14,20,0.98);
      --login-left-accent: rgba(99,102,241,0.14);
      --login-left-success: rgba(16,185,129,0.06);

      /* Spacing - fluid scale (base 4px) */
      --space-1: clamp(1.5px, 0.1875vw, 3px);
      --space-2: clamp(3px, 0.375vw, 6px);
      --space-3: clamp(6px, 0.75vw, 9px);
      --space-4: clamp(9px, 1.125vw, 12px);
      --space-5: clamp(12px, 1.5vw, 15px);
      --space-6: clamp(15px, 1.875vw, 18px);
      --space-8: clamp(21px, 2.625vw, 24px);
      --space-10: clamp(27px, 3.375vw, 30px);

      /* Typography - fluid scale */
      --font-sans: 'DM Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
      --font-display: 'Syne', var(--font-sans);
      --text-xs: clamp(0.525rem, 0.75vw, 0.5625rem);
      --text-sm: clamp(0.6rem, 0.9vw, 0.65625rem);
      --text-base: clamp(0.675rem, 1.125vw, 0.75rem);
      --text-lg: clamp(0.75rem, 1.5vw, 0.84375rem);
      --text-xl: clamp(0.84375rem, 1.875vw, 0.9375rem);
      --text-2xl: clamp(0.975rem, 2.25vw, 1.125rem);
      --text-3xl: clamp(1.2rem, 3vw, 1.5rem);

      /* Borders */
      --radius-sm: 3px;
      --radius-md: 4.5px;
      --radius-lg: 6px;
      --radius-xl: 9px;
      --radius-2xl: 12px;
      --radius-full: 9999px;

      /* Shadows */
      --shadow-sm: 0 2px 8px rgba(0, 0, 0, 0.3);
      --shadow-md: 0 8px 24px rgba(0, 0, 0, 0.4);
      --shadow-lg: 0 16px 40px rgba(0, 0, 0, 0.5);
      --shadow-xl: 0 18px 48px rgba(0, 0, 0, 0.35);
      --shadow-focus: 0 0 0 3px var(--accent-light);
      --focus-ring: 0 0 0 3px rgba(59, 130, 246, 0.12);

      /* Transitions */
      --transition-fast: 150ms ease;
      --transition-base: 250ms ease;
      --transition-slow: 350ms ease;

      /* Z-index scale */
      --z-dropdown: 100;
      --z-sticky: 200;
      --z-fixed: 300;
      --z-modal: 1000;
      --z-popover: 1100;
      --z-tooltip: 1200;

      /* Breakpoints (for use in media queries) */
      --breakpoint-sm: 640px;
      --breakpoint-md: 768px;
      --breakpoint-lg: 1024px;
      --breakpoint-xl: 1280px;
      --breakpoint-2xl: 1536px;
    }

    /* Light Theme */
    [data-theme="light"] {
      /* Colors - Core - Light Theme */
      --bg-primary: #f8f8fa;
      --bg-secondary: #f0f0f2;
      --bg-card: #ffffff;
      --bg-elevated: #f5f5f7;
      --bg-input: #efefef;
      --bg-overlay: rgba(0, 0, 0, 0.3);
      --bg-overlay-light: rgba(0, 0, 0, 0.02);

      /* Borders */
      --border-subtle: #e8e8ec;
      --border-strong: #d0d0d8;

      /* Text - Darker for better contrast */
      --text-primary: #0f0f12;
      --text-secondary: #424250;
      --text-muted: #757580;
      --text-inverse: #f0f0f8;

      /* Shadows - lighter for light theme */
      --shadow-sm: 0 2px 8px rgba(0, 0, 0, 0.1);
      --shadow-md: 0 8px 24px rgba(0, 0, 0, 0.12);
      --shadow-lg: 0 16px 40px rgba(0, 0, 0, 0.15);

      /* Light theme Connect Store Panel overrides */
      --panel-gradient: linear-gradient(180deg, #ffffff, #f9f7ff);
      --panel-overlay-light: rgba(0, 0, 0, 0.06);
      --panel-overlay-subtle: rgba(0, 0, 0, 0.035);
      --panel-overlay-very-subtle: rgba(0, 0, 0, 0.02);
      --panel-overlay-minimal: rgba(0, 0, 0, 0.03);

      /* Light theme accent UI overrides */
      --accent-text: #4f46e5;
      --accent-gradient-bg: rgba(79, 70, 229, 0.12);
      --accent-button-shadow: 0 4px 12px rgba(79, 70, 229, 0.15);

      /* Light theme success accents */
      --success-gradient: radial-gradient(circle, rgba(16, 185, 129, 0.25), rgba(16, 185, 129, 0));
      --success-text: #059669;
      --success-text-light: #047857;

      /* Light theme info box */
      --info-box-bg: rgba(79, 70, 229, 0.08);
      --info-box-border: rgba(79, 70, 229, 0.15);
      --info-box-text: #4338ca;
      --info-box-label: #6366f1;

      /* Light theme purple accents */
      --purple-gradient-bg: rgba(139, 92, 246, 0.1);

      /* Light theme login page */
      --login-card-dark: rgba(248, 248, 250, 0.98);
      --login-card-darker: rgba(240, 240, 242, 0.98);
      --login-left-accent: rgba(79, 70, 229, 0.08);
      --login-left-success: rgba(16, 185, 129, 0.05);

      /* Light theme text on accent */
      --text-on-accent: #ffffff;
      --warning-text: #b45309;
      --danger-text: #b91c1c;

      /* Light theme skeleton */
      --skeleton-gradient: linear-gradient(90deg, #f0f0f2 25%, #e8e8ec 50%, #f0f0f2 75%);
    }

    /* Smooth theme transition */
    html {
      transition: background-color var(--transition-base), color var(--transition-base);
    }

    body {
      transition: background-color var(--transition-base), color var(--transition-base);
    }

    /* ----- Base & Typography ----- */
    body {
      font-family: var(--font-sans);
      background: var(--bg-primary);
      color: var(--text-primary);
      font-size: var(--text-base);
    }
    h1, h2, h3, h4, h5, h6 {
      font-family: var(--font-display);
      font-weight: 700;
      line-height: 1.2;
    }
    a {
      color: var(--accent);
      text-decoration: none;
      transition: color var(--transition-fast);
    }
    a:hover {
      text-decoration: underline;
    }
    code, pre {
      font-family: 'SF Mono', 'Menlo', 'Monaco', 'Cascadia Code', 'Consolas', monospace;
    }

    /* ----- Scrollbar (WebKit) ----- */
    ::-webkit-scrollbar {
      width: 6px;
      height: 6px;
    }
    ::-webkit-scrollbar-track {
      background: var(--bg-secondary);
    }
    ::-webkit-scrollbar-thumb {
      background: var(--border-strong);
      border-radius: var(--radius-full);
    }
    ::-webkit-scrollbar-thumb:hover {
      background: #555;
    }

    /* ----- Focus Styles (Accessibility) ----- */
    :focus-visible {
      outline: 2px solid var(--accent);
      outline-offset: 2px;
      box-shadow: var(--shadow-focus);
    }
    :focus:not(:focus-visible) {
      outline: none;
    }
    /* Skip link for keyboard users */
    .skip-link {
      position: absolute;
      top: -40px;
      left: 0;
      background: var(--accent);
      color: white;
      padding: var(--space-2) var(--space-4);
      z-index: var(--z-tooltip);
      transition: top var(--transition-fast);
    }
    .skip-link:focus {
      top: 0;
    }

    /* ----- Animations (respect reduced motion) ----- */
    @media (prefers-reduced-motion: no-preference) {
      @keyframes fadeUp {
        from { opacity: 0; transform: translateY(12px); }
        to { opacity: 1; transform: translateY(0); }
      }
      @keyframes fadeIn {
        from { opacity: 0; }
        to { opacity: 1; }
      }
      @keyframes shimmer {
        0% { background-position: -200% 0; }
        100% { background-position: 200% 0; }
      }
      @keyframes spin {
        to { transform: rotate(360deg); }
      }
      @keyframes slideIn {
        from { transform: translateX(100%); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
      }
      @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
      }
    }
    @media (prefers-reduced-motion: reduce) {
      *,
      *::before,
      *::after {
        animation-duration: 0.01ms !important;
        animation-iteration-count: 1 !important;
        transition-duration: 0.01ms !important;
        scroll-behavior: auto !important;
      }
    }

    /* ----- Utility Classes ----- */
    /* Layout */
    .container {
      width: 100%;
      margin-left: auto;
      margin-right: auto;
      padding-left: var(--space-4);
      padding-right: var(--space-4);
      max-width: var(--breakpoint-2xl);
    }
    .flex { display: flex; }
    .inline-flex { display: inline-flex; }
    .grid { display: grid; }
    .block { display: block; }
    .hidden { display: none; }
    .flex-col { flex-direction: column; }
    .items-center { align-items: center; }
    .items-start { align-items: flex-start; }
    .items-end { align-items: flex-end; }
    .justify-center { justify-content: center; }
    .justify-between { justify-content: space-between; }
    .justify-end { justify-content: flex-end; }
    .gap-1 { gap: var(--space-1); }
    .gap-2 { gap: var(--space-2); }
    .gap-3 { gap: var(--space-3); }
    .gap-4 { gap: var(--space-4); }
    .gap-5 { gap: var(--space-5); }

    /* Spacing */
    .p-1 { padding: var(--space-1); }
    .p-2 { padding: var(--space-2); }
    .p-3 { padding: var(--space-3); }
    .p-4 { padding: var(--space-4); }
    .p-5 { padding: var(--space-5); }
    .px-1 { padding-left: var(--space-1); padding-right: var(--space-1); }
    .px-2 { padding-left: var(--space-2); padding-right: var(--space-2); }
    .px-3 { padding-left: var(--space-3); padding-right: var(--space-3); }
    .px-4 { padding-left: var(--space-4); padding-right: var(--space-4); }
    .py-1 { padding-top: var(--space-1); padding-bottom: var(--space-1); }
    .py-2 { padding-top: var(--space-2); padding-bottom: var(--space-2); }
    .py-3 { padding-top: var(--space-3); padding-bottom: var(--space-3); }
    .py-4 { padding-top: var(--space-4); padding-bottom: var(--space-4); }
    .m-0 { margin: 0; }
    /* ... add more margin/padding as needed */

    /* Typography */
    .text-xs { font-size: var(--text-xs); }
    .text-sm { font-size: var(--text-sm); }
    .text-base { font-size: var(--text-base); }
    .text-lg { font-size: var(--text-lg); }
    .text-xl { font-size: var(--text-xl); }
    .text-2xl { font-size: var(--text-2xl); }
    .text-3xl { font-size: var(--text-3xl); }
    .font-normal { font-weight: 400; }
    .font-medium { font-weight: 500; }
    .font-semibold { font-weight: 600; }
    .font-bold { font-weight: 700; }
    .text-primary { color: var(--text-primary); }
    .text-secondary { color: var(--text-secondary); }
    .text-muted { color: var(--text-muted); }
    .text-accent { color: var(--accent); }
    .text-success { color: var(--success); }
    .text-warning { color: var(--warning); }
    .text-danger { color: var(--danger); }
    .text-info { color: var(--info); }

    /* Backgrounds */
    .bg-primary { background: var(--bg-primary); }
    .bg-secondary { background: var(--bg-secondary); }
    .bg-card { background: var(--bg-card); }
    .bg-elevated { background: var(--bg-elevated); }
    .bg-input { background: var(--bg-input); }
    .bg-accent-light { background: var(--accent-light); }
    .bg-success-light { background: var(--success-light); }
    .bg-warning-light { background: var(--warning-light); }
    .bg-danger-light { background: var(--danger-light); }
    .bg-info-light { background: var(--info-light); }

    /* Borders */
    .border { border: 1px solid var(--border-strong); }
    .border-subtle { border-color: var(--border-subtle); }
    .border-strong { border-color: var(--border-strong); }
    .border-accent { border-color: var(--accent); }
    .rounded-sm { border-radius: var(--radius-sm); }
    .rounded-md { border-radius: var(--radius-md); }
    .rounded-lg { border-radius: var(--radius-lg); }
    .rounded-xl { border-radius: var(--radius-xl); }
    .rounded-2xl { border-radius: var(--radius-2xl); }
    .rounded-full { border-radius: var(--radius-full); }

    /* Effects */
    .shadow-sm { box-shadow: var(--shadow-sm); }
    .shadow-md { box-shadow: var(--shadow-md); }
    .shadow-lg { box-shadow: var(--shadow-lg); }
    .transition { transition: var(--transition-base); }
    .transition-fast { transition: var(--transition-fast); }
    .hover-lift:hover { transform: translateY(-2px); }

    /* Animations */
    .fade-up { animation: fadeUp 0.4s cubic-bezier(0.2, 0.9, 0.3, 1) forwards; }
    .fade-in { animation: fadeIn 0.3s ease forwards; }
    .slide-in { animation: slideIn 0.3s ease forwards; }
    .spin { animation: spin 1s linear infinite; }
    .pulse { animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite; }

    /* Icons */
    .icon {
      display: inline-block;
      width: 1em;
      height: 1em;
      vertical-align: middle;
      fill: currentColor;
      stroke: currentColor;
    }
    .icon-xs { font-size: 12px; }
    .icon-sm { font-size: 14px; }
    .icon-md { font-size: 18px; }
    .icon-lg { font-size: 24px; }
    .icon-xl { font-size: 32px; }

    /* Image zoom on hover (used in ProductCard) */
    .img-zoom-wrapper {
      overflow: hidden;
      display: block;
    }
    .img-zoom {
      transition: transform var(--transition-base);
      will-change: transform;
    }
    .img-zoom-wrapper:hover .img-zoom {
      transform: scale(1.08);
    }
    /* Image overlay */
    .img-overlay {
      position: absolute;
      inset: 0;
      background: var(--bg-overlay);
      opacity: 0;
      transition: opacity var(--transition-base);
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .img-overlay:hover,
    .img-overlay:focus-within {
      opacity: 1;
    }

    /* App shell layout */
    .app-shell {
      display: flex;
      min-height: 100vh;
      background: var(--bg-primary);
      overflow-x: hidden;
    }
    .app-main {
      flex: 1;
      margin-left: 195px;
      width: calc(100vw - 195px);
      min-height: 100vh;
    }
    .app-sidebar-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.45);
      border: 0;
      z-index: 1300;
      cursor: pointer;
    }
    .app-mobile-header {
      position: sticky;
      top: 0;
      z-index: var(--z-sticky);
      display: flex;
      align-items: center;
      gap: 7.5px;
      padding: 7.5px 10.5px;
      margin-bottom: 7.5px;
      background: var(--bg-secondary);
      border-bottom: 1px solid var(--border-subtle);
      backdrop-filter: blur(8px);
    }
    .app-mobile-header h1 {
      font-size: var(--text-lg);
      font-family: var(--font-display);
      color: var(--text-primary);
      margin: 0;
    }
    .app-mobile-menu-btn {
      width: 25.5px;
      height: 25.5px;
      border-radius: var(--radius-md);
      border: 1px solid var(--border-strong);
      background: var(--bg-card);
      color: var(--text-primary);
      display: inline-flex;
      align-items: center;
      justify-content: center;
      cursor: pointer;
      transition: border-color var(--transition-fast), background var(--transition-fast);
    }
    .app-mobile-menu-btn:hover {
      background: var(--bg-elevated);
      border-color: var(--accent);
    }

    @media (max-width: 1024px) {
      .app-main {
        margin-left: 0;
        width: 100vw;
      }
      .app-sidebar {
        z-index: 1400;
      }
    }

    /* Card styles */
    .card {
      background: var(--bg-card);
      border-radius: var(--radius-xl);
      overflow: hidden;
      transition: transform var(--transition-base), box-shadow var(--transition-base), border-color var(--transition-base);
    }
    .card-outline {
      border: 1px solid var(--border-strong);
    }
    .card-elevated {
      box-shadow: var(--shadow-sm);
    }
    .card-hover:hover {
      transform: translateY(-4px);
      box-shadow: var(--shadow-md);
      border-color: var(--accent) !important;
    }

    /* ----- Component Base Styles ----- */
    /* Buttons */
    .btn {
      border: none;
      cursor: pointer;
      font-family: inherit;
      font-weight: 600;
      transition: var(--transition-fast);
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: var(--space-2);
      border-radius: var(--radius-lg);
      padding: var(--space-3) var(--space-5);
      font-size: var(--text-sm);
      line-height: 1;
      white-space: nowrap;
    }
    .btn:active { transform: scale(0.97); }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn-sm { padding: var(--space-2) var(--space-3); font-size: var(--text-xs); border-radius: var(--radius-md); }
    .btn-lg { padding: var(--space-4) var(--space-6); font-size: var(--text-base); border-radius: var(--radius-xl); }
    .btn-icon { padding: var(--space-2); border-radius: var(--radius-full); aspect-ratio: 1; }

    .btn-primary {
      background: var(--accent-gradient);
      color: white;
    }
    .btn-primary:hover:not(:disabled) {
      opacity: 0.9;
      box-shadow: var(--shadow-accent-lg);
    }

    .btn-secondary {
      background: var(--bg-card);
      border: 1px solid var(--border-strong);
      color: var(--text-secondary);
    }
    .btn-secondary:hover:not(:disabled) {
      background: var(--bg-elevated);
      border-color: var(--accent);
      color: var(--text-primary);
    }

    .btn-danger {
      background: var(--danger-light);
      color: var(--danger);
      border: 1px solid var(--danger-border-light);
    }
    .btn-danger:hover:not(:disabled) {
      background: var(--danger-bg-hover);
      border-color: var(--danger);
    }

    .btn-ghost {
      background: transparent;
      color: var(--text-secondary);
    }
    .btn-ghost:hover:not(:disabled) {
      background: var(--bg-overlay-light);
      color: var(--text-primary);
    }

    /* Form inputs */
    .field-input {
      background: var(--bg-input);
      border: 1px solid var(--border-strong);
      color: var(--text-primary);
      outline: none;
      transition: border-color var(--transition-fast), box-shadow var(--transition-fast);
      font-family: inherit;
      width: 100%;
      border-radius: var(--radius-md);
      padding: var(--space-3) var(--space-4);
      font-size: var(--text-sm);
    }
    .field-input:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 3px var(--accent-light);
    }
    .field-input::placeholder { color: var(--text-muted); }
    .field-input:disabled { opacity: 0.5; cursor: not-allowed; }

    /* Select */
    select.field-input {
      appearance: none;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%23b0b0c0' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'%3E%3C/polyline%3E%3C/svg%3E");
      background-repeat: no-repeat;
      background-position: right var(--space-3) center;
      background-size: 16px;
      padding-right: var(--space-8);
    }

    /* Checkbox & Radio */
    .chk, .radio {
      appearance: none;
      width: 20px;
      height: 20px;
      border: 2px solid var(--border-strong);
      cursor: pointer;
      background: var(--bg-input);
      position: relative;
      transition: var(--transition-fast);
      display: inline-flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
    }
    .chk:hover:not(:checked), .radio:hover:not(:checked) {
      border-color: var(--accent);
      background: var(--accent-light);
    }
    .chk { border-radius: var(--radius-sm); }
    .radio { border-radius: var(--radius-full); }
    .chk:checked, .radio:checked {
      background: var(--accent);
      border-color: var(--accent);
    }
    .chk:checked::after {
      content: '✓';
      color: white;
      font-size: 13px;
      line-height: 1;
      font-weight: 700;
    }
    .radio:checked::after {
      content: '';
      width: 10px;
      height: 10px;
      background: white;
      border-radius: var(--radius-full);
    }
    .chk:focus-visible, .radio:focus-visible {
      outline: 2px solid var(--accent);
      outline-offset: 2px;
    }
    .chk:active, .radio:active {
      transform: scale(0.95);
    }

    /* Switch / Toggle */
    .switch {
      position: relative;
      display: inline-block;
      width: 40px;
      height: 24px;
    }
    .switch input {
      opacity: 0;
      width: 0;
      height: 0;
    }
    .slider {
      position: absolute;
      cursor: pointer;
      inset: 0;
      background: var(--border-strong);
      transition: var(--transition-fast);
      border-radius: var(--radius-full);
    }
    .slider:before {
      position: absolute;
      content: '';
      height: 18px;
      width: 18px;
      left: 3px;
      bottom: 3px;
      background: white;
      transition: var(--transition-fast);
      border-radius: 50%;
    }
    input:checked + .slider {
      background: var(--accent);
    }
    input:checked + .slider:before {
      transform: translateX(16px);
    }

    /* Badge (if not already in UI) */
    .badge {
      display: inline-flex;
      align-items: center;
      padding: var(--space-1) var(--space-2);
      border-radius: var(--radius-full);
      font-size: var(--text-xs);
      font-weight: 600;
      line-height: 1;
    }
    .badge-success { background: var(--success-light); color: var(--success); }
    .badge-warning { background: var(--warning-light); color: var(--warning); }
    .badge-danger { background: var(--danger-light); color: var(--danger); }
    .badge-info { background: var(--info-light); color: var(--info); }
    .badge-accent { background: var(--accent-light); color: var(--accent); }

    /* Skeleton loading */
    .skeleton {
      background: var(--skeleton-gradient);
      background-size: 200% 100%;
      animation: shimmer 1.5s infinite;
      border-radius: var(--radius-md);
    }

    /* Modal */
    .modal-overlay {
      position: fixed;
      inset: 0;
      z-index: var(--z-modal);
      background: rgba(0, 0, 0, 0.8);
      backdrop-filter: blur(8px);
      display: flex;
      align-items: center;
      justify-content: center;
      padding: var(--space-5);
    }
    .modal-box {
      background: var(--bg-card);
      border: 1px solid var(--border-strong);
      border-radius: var(--radius-2xl);
      width: 100%;
      max-width: 600px;
      max-height: calc(100vh - var(--space-10));
      overflow-y: auto;
      box-shadow: var(--shadow-lg);
      animation: fadeUp 0.3s ease;
    }
    .modal-box::-webkit-scrollbar { width: 4px; }

    /* Tooltip */
    .tooltip {
      position: relative;
    }
    .tooltip:hover:after {
      content: attr(data-tooltip);
      position: absolute;
      bottom: 100%;
      left: 50%;
      transform: translateX(-50%);
      background: var(--bg-elevated);
      color: var(--text-primary);
      padding: var(--space-1) var(--space-2);
      border-radius: var(--radius-sm);
      font-size: var(--text-xs);
      white-space: nowrap;
      z-index: var(--z-tooltip);
      border: 1px solid var(--border-strong);
    }
  `}</style>
);

export default GlobalStyles;
