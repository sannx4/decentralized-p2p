# OffPeer
# Decentralized Peer-to-Peer File Synchronization System

## 📌 Overview
A decentralized file synchronization system built using **Conflict-free Replicated Data Types (CRDTs)**.  
The project ensures **real-time, conflict-free updates** across multiple peers, even under unreliable networks.  
It uses **peer-to-peer networking (ICE/STUN/TURN)** for NAT traversal and a **gossip protocol** for multi-peer state propagation.  
A **React + FastAPI dashboard** is included for monitoring and peer management.  

---

## 🚀 Features
- 🔄 CRDT-based sync engine for eventual consistency  
- 🌐 Peer-to-peer networking with NAT traversal (ICE/STUN/TURN)  
- 📡 Multi-peer gossip protocol for scalable state propagation  
- 🔒 End-to-end encryption for secure sync  
- 📊 Dashboard (React + FastAPI) for real-time monitoring  

---

## 🛠️ Tech Stack
- **Backend:** Python (asyncio, aioice), FastAPI  
- **Core:** CRDT engine, Gossip protocol  
- **Networking:** WebRTC ICE (STUN/TURN)  
  

