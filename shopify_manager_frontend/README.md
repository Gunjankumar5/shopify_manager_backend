# Shopify Manager Frontend

React + Vite frontend for managing Shopify products, collections, inventory, and bulk uploads with multi-store support and user authentication.

## Tech Stack

- React 18
- Vite 5
- Tailwind CSS 3
- PostCSS + Autoprefixer
- Supabase (authentication)
- Handsontable (spreadsheet component)

## Features

- **Product Management** — List, search, create, edit, delete, and sync products from Shopify. Bulk price adjustment and duplicate removal.
- **Collections Management** — CRUD for custom collections with image upload support.
- **Inventory Management** — View locations and inventory levels, update quantities by location.
- **Bulk Upload** — Import CSV/Excel files, preview & edit data, then push to Shopify with duplicate handling.
- **Multi-Store Support** — Connect and switch between multiple Shopify stores with automatic refetch on switch.
- **User Management** — Role-based access control (Admin, Manager, Junior) with granular permissions (admin/manager only).
- **Data Export** — Export product data as Excel or JSON with sync helper utilities.
- **Session Persistence** — Stay logged in across browser reloads via Supabase session management.

## Project Structure

```
frontend/
├── package.json                # npm dependencies
├── vite.config.js              # Vite bundler config
├── tailwind.config.js          # Tailwind CSS theme
├── index.html                  # HTML entry point
├── public/                     # Static assets
│
└── src/
    ├── App.jsx                 # Root layout and page switching
    ├── main.jsx                # React mount root
    ├── index.css               # Tailwind CSS entry
    │
    ├── api/
    │   ├── api.js              # Request helpers, caching, error handling
    │   └── config.js           # API base URL configuration
    │
    ├── lib/
    │   ├── supabaseClient.js   # Supabase auth client
    │   └── authFetch.js        # Fetch wrapper with Bearer token
    │
    ├── context/
    │   └── ThemeContext.jsx    # Theme state management
    │
    ├── components/             # Reusable UI components
    │   ├── GlobalStyles.jsx    # CSS variables and global styles
    │   ├── Sidebar.jsx         # Navigation and store switcher
    │   ├── ProductCard.jsx     # Product grid card
    │   ├── Toast.jsx           # Toast notifications
    │   ├── Modals.jsx          # Modal dialogs
    │   ├── Icons.jsx           # SVG icon set
    │   └── UI.jsx              # Button, input, dropdown atoms
    │
    └── pages/                  # Full-page components
        ├── LoginPage.jsx       # Supabase auth entry
        ├── ProductsPage.jsx    # Product management
        ├── UploadPage.jsx      # Bulk upload workflow
        ├── CollectionsPage.jsx # Collection management
        ├── InventoryPage.jsx   # Inventory management
        ├── ExportPage.jsx      # Handsontable grid export
        ├── ConnectStore.jsx    # Store connection (OAuth)
        ├── Addproductpage.jsx  # Create/edit products
        ├── AddcollectionPage.jsx # Create/edit collections
        └── UserManagementPage.jsx # User roles & permissions
```

## Prerequisites

- Node.js 18+
- npm 9+
- Supabase account with authentication configured (see [SUPABASE_SETUP.md](../SUPABASE_SETUP.md))
- Backend API running on localhost:8000 or configured API origin

## Environment Setup

### 1. Create `.env` File

Create `frontend/.env` in the project root:

```env
# Supabase Configuration
VITE_SUPABASE_URL=https://your-project-id.supabase.co
VITE_SUPABASE_ANON_KEY=your-anon-key-here

# Backend API (Development)
VITE_API_ORIGIN=http://localhost:8000

# For Production
# VITE_API_ORIGIN=https://your-backend-domain.com
```

**Requirements:**

- `VITE_SUPABASE_URL`: Your Supabase project URL (from Settings → API)
- `VITE_SUPABASE_ANON_KEY`: Your Supabase anon public key (from Settings → API)
- `VITE_API_ORIGIN`: Backend API URL (defaults to `http://localhost:8000` if omitted)

**Important:** Vite environment variables must start with `VITE_` to be exposed to client code. Variables without this prefix are ignored.

For detailed Supabase setup, see [SUPABASE_SETUP.md](../SUPABASE_SETUP.md).

### 2. Install Dependencies

```bash
npm install
```

## Running the Application

### Development Server

```bash
npm run dev
```

This starts the Vite dev server, typically at `http://localhost:5173`.

### Production Build

```bash
npm run build
```

Creates optimized production build in `dist/` folder.

### Preview Production Build

```bash
npm run preview
```

Serves the production build locally for testing.

## Authentication Flow

### User Login

1. User visits app and is not authenticated
2. Frontend shows `LoginPage.jsx` with Supabase login form
3. User enters email and password (or signs up)
4. Supabase authenticates and returns session with Bearer token
5. Frontend stores session in local storage
6. App checks `supabase.auth.getSession()` on startup
7. If session exists, user is automatically logged in
8. If session is missing or invalid, user sees login page

### Bearer Token Management

- `lib/supabaseClient.js` handles session with:
  - `autoRefreshToken: true` - Auto-refresh expired tokens
  - `persistSession: true` - Save session to local storage
  - `detectSessionInUrl: true` - Detect OAuth redirects
- `lib/authFetch.js` adds Bearer token to every API request header
- If token expires, backend returns `401 Unauthorized`
- Frontend then redirects user to login page

### Session Restoration

On app startup, `App.jsx`:

1. Calls `supabase.auth.getSession()` to restore saved session
2. If session found, restores user and sets `isLoggedIn = true`
3. If no session, shows login page
4. User stays logged in across browser reloads (until session expires)

## Key Features Explained

### Multi-Store Support

- User authenticates via Supabase once
- In sidebar, user can see all connected Shopify stores
- Clicking a store in sidebar calls backend to set active store
- Products page automatically refetches data for selected store
- Cache is cleared on store switch to prevent stale data

### Page Structure

Single-page React app with internal page state (not React Router):

- `App.jsx` manages `currentPage` state
- Sidebar triggers page changes via setCurrentPage()
- Each page receives necessary props (activeStore, setActiveStore, etc.)
- Toast notifications appear globally

### Data Caching

Request data is cached client-side:

- GET requests cached for 30 seconds
- Cache key includes request URL + params
- Cache cleared on store switch (via `api.clearCache()`)
- Mutations (POST/PUT/DELETE) clear cache automatically

### Styling with Tailwind

- `GlobalStyles.jsx` exports CSS classes for design tokens
- All components use Tailwind utility classes
- CSS variables defined in `GlobalStyles.jsx` for theme consistency
- Dark mode support built-in via Tailwind

### User Management

When authenticated user is an Admin or Manager:

- "Users" link appears in sidebar
- User can view, create, and manage junior users
- Junior user permissions can be customized per feature
- Admins can deactivate users (soft delete)
- Managers have limited user management capabilities

See [USER_MANAGEMENT.md](../USER_MANAGEMENT.md) for detailed permissions.

## API Integration

### Request Handling

`api/api.js` provides request helpers with:

- Built-in timeout (30 seconds default)
- Automatic Bearer token attachment (via `authFetch.js`)
- GET request caching (30 seconds by default)
- Automatic error extraction from response
- JSON parsing and error messages

### Making Requests

```javascript
import { api } from "./api/api";

// GET (cached)
const data = await api.get("/api/products/");

// POST (clears cache)
const result = await api.post("/api/products/", { title: "New Product" });

// PUT (clears cache)
const updated = await api.put("/api/products/1", { title: "Updated" });

// DELETE (clears cache)
await api.delete("/api/products/1");

// Clear cache manually
api.clearCache();
```

### Error Handling

API errors are extracted to a `message` property:

```javascript
try {
  await api.post("/api/products/", data);
} catch (error) {
  console.error(error.message); // Auto-extracted error message
}
```

## Tailwind CSS Customization

Theme configuration in `tailwind.config.js`:

- Define custom colors, spacing, typography
- Theme extends default Tailwind config
- Variantsinclude dark mode, focus states, etc.

CSS variables in `GlobalStyles.jsx`:

```css
:root {
  --bg-primary: #ffffff;
  --bg-secondary: #f3f4f6;
  --text-primary: #1f2937;
  --accent: #3b82f6;
  /* ... more variables ... */
}
```

## Available Scripts

| Command           | Description                      |
| ----------------- | -------------------------------- |
| `npm run dev`     | Start Vite dev server            |
| `npm run build`   | Create production build          |
| `npm run preview` | Preview production build locally |

## Deployment

### Vercel

The frontend includes `vercel.json` for Vercel deployment:

1. Connect your GitHub repository to Vercel
2. Set environment variables in Vercel project settings:
   - `VITE_SUPABASE_URL`
   - `VITE_SUPABASE_ANON_KEY`
   - `VITE_API_ORIGIN` (point to production backend)
3. Deploy via Vercel dashboard or auto-deploy on git push

### Other Platforms (Netlify, GitHub Pages, etc.)

1. Run `npm run build` to create `dist/` folder
2. Deploy the `dist/` folder to your hosting service
3. Set environment variables in hosting platform settings
4. Ensure `VITE_API_ORIGIN` points to your production backend

## Troubleshooting

### "Cannot find Supabase module"

- Run `npm install` (may need to reinstall node_modules)
- Check that `supabase` is in `package.json` dependencies
- Verify you're in the `frontend/` directory

### API requests return 401 Unauthorized

- Check that Bearer token is in Authorization header
- Verify token is valid and not expired
- Check `lib/authFetch.js` is adding Bearer token correctly
- Confirm backend Supabase configuration matches frontend

### Frontend can't reach backend

- Verify backend is running on correct port
- Check `VITE_API_ORIGIN` in `.env` is correct
- For localhost development, use `http://localhost:8000`
- Check browser console for CORS errors

### Blank page after login

- Open browser DevTools (F12 → Console)
- Check for JavaScript errors
- Verify `VITE_SUPABASE_URL` and `VITE_SUPABASE_ANON_KEY` are correct
- Check network tab to see if API requests are succeeding

### Store switcher not working

- Ensure backend is responding to active-store endpoint
- Check network tab in DevTools for errors
- Verify permissions include store access

## Related Documentation

- [SUPABASE_SETUP.md](../SUPABASE_SETUP.md) - Supabase authentication setup
- [USER_MANAGEMENT.md](../USER_MANAGEMENT.md) - User roles and permissions
- [backend/README.md](../backend/README.md) - Backend API documentation
- [PROJECT_DOCUMENTATION.md](../PROJECT_DOCUMENTATION.md) - Full architecture overview
  | `npm start` | Alias for Vite dev server |

## Notes

- This repository contains the frontend only.
- Ensure the backend is running at the origin specified in `.env`.
- If CORS errors occur, allow requests from your Vite origin on the backend.

- Dev server fails to start:
  - Run `npm install` again and retry `npm run dev`
  - Check Node.js version (`node -v`)
- API requests fail:
  - Verify backend is running on `http://localhost:8000`
  - Confirm routes exist and return valid JSON
