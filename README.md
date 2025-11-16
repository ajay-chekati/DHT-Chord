# Chord-Based Distributed Hash Table System

This project implements a Chord-based Distributed Hash Table (DHT) from scratch.  
Nodes form a Chord ring, store key–value metadata, and support operations such as file publishing, lookup, and visualization.

---

## Features
- Custom implementation of the Chord DHT protocol  
- Consistent hashing for key distribution  
- Routing through finger tables  
- File indexing and lookup using DHT keys  
- Direct file storage and retrieval between nodes  
- Scripts for launching nodes, generating test data, and visualizing the ring  

---

## How to Run

1. **Create 20 nodes on ports starting from 6000**
   ```bash
   ./scripts/launch_nodes.sh 20 6000
   ```

2. **Visualize the Chord ring**
   ```bash
   python ./scripts/visualize_ring.py --host 127.0.0.1 --bootstrap 6000 --max-nodes 1024
   ```

3. **Generate 100 test text files**
   ```bash
   python ./scripts/generate_files.py
   ```

4. **Publish all files in the `scripts/files` folder**
   ```bash
   ./scripts/publish_folder.py
   ```

5. **Start nodes manually**
   - Start a node
     ```bash
     python cli.py start --host 127.0.0.1 --port 6000
     ```
   - Start another node and connect it to the ring
     ```bash
     python cli.py start --host 127.0.0.1 --port 6001 --bootstrap 127.0.0.1:6000
     ```

6. **Publish a file**
   ```bash
   python cli.py put --host 127.0.0.1 --port 6000 --file ./scripts/files/doc_01.txt
   ```

7. **Look up a file by key**
   ```bash
   python cli.py get --host 127.0.0.1 --port 6002 --key 1217246485
   ```

8. **Find the PID of the process running on a port**
   ```bash
   lsof -ti tcp:6005
   ```

9. **Kill the process**
   ```bash
   kill <pid>
   ```
