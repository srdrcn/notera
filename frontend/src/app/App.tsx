import { BrowserRouter } from "react-router-dom";

import { AppRouter } from "../routes/AppRouter";


export function App() {
  return (
    <BrowserRouter>
      <AppRouter />
    </BrowserRouter>
  );
}
