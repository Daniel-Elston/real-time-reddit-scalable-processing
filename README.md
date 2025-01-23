# README

### **QRG:**

1. Load up docker app
2. Load 2 separate WSL terminals (T1 and T2)
3. In T1, run ``docker-compose up --build``
4. Once all images are running, in T2, run ``python main.py``
5. Data is streamed in temrinal but also saved: ``data/temp/reddit_comments.json``

---

### **Requirements:**
- WSL2
- Ubuntu 24.04
- Python 3.12.*

---

### Ensure Java Runtime Env:

sudo apt update
sudo apt install openjdk-11-jdk

readlink -f $(which java)

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

**Add lines to .zshrc:**
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc