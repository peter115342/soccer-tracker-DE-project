/* eslint-disable @typescript-eslint/no-unused-vars */
// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { getAnalytics } from "firebase/analytics";
import { getFirestore } from 'firebase/firestore';


// For Firebase JS SDK v7.20.0 and later, measurementId is optional
const firebaseConfig = {
   // apiKey: import.meta.env.PUBLIC_FIREBASE_API_KEY,
    authDomain: "vigilant-shell-435820-r2.firebaseapp.com",
    projectId: "vigilant-shell-435820-r2",
    storageBucket: "vigilant-shell-435820-r2.firebasestorage.app",
   // messagingSenderId: import.meta.env.PUBLIC_FIREBASE_MESSAGING_SENDER_ID,
    //appId: import.meta.env.PUBLIC_FIREBASE_APP_ID
};



// Initialize Firebase
const app = initializeApp(firebaseConfig);
export const db = getFirestore(app);
const analytics = getAnalytics(app);




