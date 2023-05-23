import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./index.css";
import { initializeApp } from "firebase/app";
import { getAnalytics } from "firebase/analytics";

const firebaseConfig = {
  apiKey: "AIzaSyBsoxGLtbLeRTGsMjRQSxd99or4aIYTfQk",
  authDomain: "isolation-test-90557.firebaseapp.com",
  projectId: "isolation-test-90557",
  storageBucket: "isolation-test-90557.appspot.com",
  messagingSenderId: "801169744551",
  appId: "1:801169744551:web:6c6b5de5239aed4115f182",
};

const app = initializeApp(firebaseConfig);
getAnalytics(app);

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
