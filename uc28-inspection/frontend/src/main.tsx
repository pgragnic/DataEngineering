import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { QueryProvider } from "@/lib/query-provider";
import "@/styles/globals.css";
import Home from "./pages/Home";
import Brief from "./pages/Brief";
import Capture from "./pages/Capture";
import Report from "./pages/Report";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/inspection/:id/brief" element={<Brief />} />
          <Route path="/inspection/:id/capture" element={<Capture />} />
          <Route path="/inspection/:id/report" element={<Report />} />
        </Routes>
      </BrowserRouter>
    </QueryProvider>
  </React.StrictMode>
);
