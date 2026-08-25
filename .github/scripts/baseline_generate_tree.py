import json
import os
import re
from collections import defaultdict

input_file = './falco-data/falco_events.json'
baseline_file = './falco-data/baseline.json'
hashes_file = './falco-data/hashes.txt'
artifact_summary_file = './falco-data/security_summary.md'
events = []

try:
    with open(input_file, 'r') as f:
        for line in f:
            if line.strip():
                events.append(json.loads(line))
except FileNotFoundError:
    print(f"No {input_file} found. Skipping summary.")
    exit(0)

# --- UTILITY FUNCTIONS FOR PARSING ---
def sanitize_cmd(cmd_string):
    if not cmd_string: return "unknown"
    return cmd_string.strip().split()[0].split('/')[-1]

INTERPRETERS = {"python", "python3", "node", "nodejs", "perl", "ruby", "php"}

processes = {}
children_map = defaultdict(list)
parent_map = {}
connection_map = defaultdict(set)

privesc_pids = set()
tty_pids = set()
sensitive_reads = defaultdict(set) 
env_reads = defaultdict(set)
mem_reads = defaultdict(set)
staging_pids = set()
exfil_net_pids = set()
lotl_pids = set()

for event in events:
    rule_name = event.get('rule')
    fields = event.get('output_fields', {})
    pid = fields.get('proc.pid')
    ppid = fields.get('proc.ppid')
    cmdline = fields.get('proc.cmdline')
    
    # FIX: Use 'or ""' to prevent NoneType errors if fd.name explicitly returns null
    raw_fd = fields.get('fd.name') or '' 

    filename = None
    if "->" in raw_fd:
        dest_part = raw_fd.split("->")[1].strip()
        dest_ip = dest_part.split(":")[0]
        connection_map[pid].add(dest_ip)
    elif raw_fd:
        filename = raw_fd

    if not pid: continue
    
    if cmdline: 
        processes[pid] = cmdline
        
    if ppid:
        parent_map[pid] = ppid
        if pid not in children_map[ppid]: 
            children_map[ppid].append(pid)
            
    # Map to both old and new behavioral rules
    if rule_name in ('Detect Privilege Escalation', 'Detect Privilege Escalation (Behavioral Syscall)'): 
        privesc_pids.add(pid)
    if rule_name == 'Detect Sensitive File Read' and filename: sensitive_reads[pid].add(filename)
    if rule_name in ('Detect Interactive or Reverse Shell', 'Detect True Reverse Shell (Behavioral Syscall)'): 
        tty_pids.add(pid)
    if rule_name == 'Detect Environment Variable Access' and filename: env_reads[pid].add(filename)
    if rule_name == 'Detect Memory Access via Procfs' and filename: mem_reads[pid].add(filename)
    if rule_name in ('Detect Data Staging and Encryption', 'Detect Suspicious Dropper/Staging Activity'): 
        staging_pids.add(pid)
        
    # --- DYNAMIC CORRELATION LOGIC ---
    if rule_name in ('Detect Suspicious Network Exfiltration', 'Detect Outbound Network Activity', 'Detect Interpreter Network Connection'): 
        exfil_net_pids.add(pid)
        
        # If the process establishing the connection is an interpreter, flag it as LotL Abuse automatically
        binary_name = sanitize_cmd(cmdline)
        if binary_name in INTERPRETERS:
            lotl_pids.add(pid)
            
    if rule_name == 'Detect Suspicious Interpreter Inline Execution': 
        lotl_pids.add(pid)


# --- 1. EXTRACT HASHES FROM JSONL ARTIFACT ---
current_hashes_map = {} 
if os.path.exists(hashes_file):
    with open(hashes_file, 'r') as f:
        for line in f:
            if line.strip():
                try:
                    data = json.loads(line)
                    if "sha256" in data:
                        current_hashes_map[data["sha256"]] = data.get("filename", "Unknown")
                except json.JSONDecodeError:
                    pass

current_hashes = set(current_hashes_map.keys())

# --- 2. EXTRACT DOMAINS & IPs FROM ALL COMMANDS AND KERNEL SOCKETS ---
current_network_targets = set()

for cmd in processes.values():
    urls = re.findall(r'https?://([a-zA-Z0-9.-]+)', cmd)
    for url in urls: current_network_targets.add(url)

for ips in connection_map.values():
    for ip in ips: current_network_targets.add(ip)

# --- 3. EXTRACT SANITIZED PROCESS LINEAGES ---
def get_network_target(cmd_string, pid):
    binary = sanitize_cmd(cmd_string)
    if pid in connection_map and connection_map[pid]:
        targets = ", ".join(connection_map[pid])
        return f"{binary} ({targets})"
    urls = re.findall(r'https?://([a-zA-Z0-9.-]+)', cmd_string)
    if urls: return f"{binary} ({urls[0]})"
    return binary

current_lineages = set()
for pid in processes.keys():
    path = []
    curr = pid
    seen = set()
    
    while curr in processes and curr not in seen:
        seen.add(curr)
        path.append(sanitize_cmd(processes[curr]))
        curr = parent_map.get(curr)
    
    if path:
        current_lineages.add(" -> ".join(reversed(path)))


# --- HYBRID BASELINE LOGIC (FREQUENCY + QUALITATIVE) ---
current_alert_details = {
    "Privilege Escalation": {},
    "Interactive Shell / TTY": {},
    "Environment Variable Scraping": {},
    "Memory Scraping via Procfs": {},
    "Sensitive File Reads": {},
    "Data Staging / Encryption": {},
    "Network Exfiltration": {},
    "Interpreter Abuse (LotL)": {}
}

def increment_freq(alert_category, item):
    current_alert_details[alert_category][item] = current_alert_details[alert_category].get(item, 0) + 1

for pid in privesc_pids: increment_freq("Privilege Escalation", sanitize_cmd(processes.get(pid, "")))
for pid in tty_pids: increment_freq("Interactive Shell / TTY", sanitize_cmd(processes.get(pid, "")))
for pid in staging_pids: increment_freq("Data Staging / Encryption", sanitize_cmd(processes.get(pid, "")))
for pid in lotl_pids: increment_freq("Interpreter Abuse (LotL)", sanitize_cmd(processes.get(pid, "")))
for pid in exfil_net_pids: increment_freq("Network Exfiltration", get_network_target(processes.get(pid, ""), pid))

for pid, files in env_reads.items():
    for f in files: increment_freq("Environment Variable Scraping", f)
for pid, files in mem_reads.items():
    for f in files: increment_freq("Memory Scraping via Procfs", f)
for pid, files in sensitive_reads.items():
    for f in files: increment_freq("Sensitive File Reads", f)

baseline_alert_details = {}
baseline_domains = set()
baseline_hashes = set()
baseline_lineages = set()

if os.path.exists(baseline_file):
    try:
        with open(baseline_file, 'r') as f:
            data = json.load(f)
            if "alerts" in data:
                for k, v in data.get("alerts", {}).items():
                    if isinstance(v, dict): baseline_alert_details[k] = v
                    elif isinstance(v, list): baseline_alert_details[k] = {item: 1 for item in v}
                    else: baseline_alert_details[k] = {}
                baseline_domains = set(data.get("domains", []))
                baseline_hashes = set(data.get("hashes", []))
                baseline_lineages = set(data.get("lineages", []))
    except:
        pass

new_deviations = []

for alert_name, current_items in current_alert_details.items():
    baseline_items = baseline_alert_details.get(alert_name, {})
    for item, current_count in current_items.items():
        baseline_count = baseline_items.get(item, 0)
        if current_count > baseline_count:
            if baseline_count == 0:
                new_deviations.append(f"- **{alert_name}**: New indicator `{item}` detected (ran {current_count} times).")
            else:
                new_deviations.append(f"- **{alert_name}**: Frequency increase for `{item}` (ran {current_count} times, previously {baseline_count}).")

new_domains = current_network_targets - baseline_domains
new_hashes = current_hashes - baseline_hashes
new_lineages = current_lineages - baseline_lineages 

if new_domains:
    new_deviations.append("- **New Network Targets Contacted**: " + ", ".join(f"`{d}`" for d in new_domains))
if new_hashes:
    hash_details = [f"`{current_hashes_map.get(h, 'Unknown')}` ({h[:8]}...)" for h in new_hashes]
    new_deviations.append(f"- **New Executable Hashes Detected**: {', '.join(hash_details)}")
if new_lineages:
    new_deviations.append(f"- **Behavioral Drift**: `{len(new_lineages)}` new execution paths detected:")
    for lin in list(new_lineages)[:10]: new_deviations.append(f"  - `{lin}`")

new_baseline = {
    "alerts": current_alert_details, 
    "domains": list(current_network_targets),
    "hashes": list(current_hashes),
    "lineages": list(current_lineages)
}
with open(baseline_file, 'w') as f:
    json.dump(new_baseline, f)

def format_for_display(cmd_string, max_len=120):
    if not cmd_string: return "Unknown"
    clean_cmd = re.sub(r'\s+', ' ', cmd_string).strip()
    if len(clean_cmd) > max_len: return clean_cmd[:max_len] + "..."
    return clean_cmd

all_pids = set(processes.keys())
roots = set(children_map.keys()) - all_pids

summary = []

if new_deviations:
    summary.append("### ⚠️ SECURITY BASELINE DEVIATION DETECTED ⚠️")
    summary.append("This workflow run generated new IoCs, behaviors, or security alerts than the previous successful run. **Review the deviations below.**\n")
    summary.extend(new_deviations)
    summary.append("\n---\n")

if privesc_pids:
    summary.append("### 🚨 CRITICAL: Privilege Escalation Detected 🚨")
    summary.append("| PID | Command |")
    summary.append("|---|---|")
    for esc_pid in privesc_pids: summary.append(f"| {esc_pid} | `{format_for_display(processes.get(esc_pid))}` |")
    summary.append("\n---\n")

if tty_pids:
    summary.append("### 💀 FATAL: Interactive Shell / TTY Spawned 💀")
    summary.append("| PID | Command |")
    summary.append("|---|---|")
    for shell_pid in tty_pids: summary.append(f"| {shell_pid} | `{format_for_display(processes.get(shell_pid))}` |")
    summary.append("\n---\n")

if lotl_pids:
    summary.append("### 🐍 CRITICAL: Interpreter Abuse (LotL) / Interpreter Network Connection 🐍")
    summary.append("An interpreter process (Python, Node, etc.) exhibited highly suspicious behavior, such as establishing an unauthorized network connection.")
    summary.append("| PID | Command | Target IPs (Resolved) |")
    summary.append("|---|---|---|")
    for l_pid in lotl_pids: 
        targets = ", ".join(f"`{ip}`" for ip in connection_map.get(l_pid, ["Unknown (Check Logs)"]))
        summary.append(f"| {l_pid} | `{format_for_display(processes.get(l_pid))}` | {targets} |")
    summary.append("\n---\n")

if env_reads:
    summary.append("### ☢️ DANGER: Environment Variable Scraping ☢️")
    summary.append("| PID | Command | Target File |")
    summary.append("|---|---|---|")
    for env_pid, files in env_reads.items():
        summary.append(f"| {env_pid} | `{format_for_display(processes.get(env_pid))}` | {', '.join(f'`{f}`' for f in files)} |")
    summary.append("\n---\n")

if mem_reads:
    summary.append("### 🧠 DANGER: Memory Access via Procfs 🧠")
    summary.append("| PID | Command | Target File |")
    summary.append("|---|---|---|")
    for mem_pid, files in mem_reads.items():
        summary.append(f"| {mem_pid} | `{format_for_display(processes.get(mem_pid))}` | {', '.join(f'`{f}`' for f in files)} |")
    summary.append("\n---\n")

if sensitive_reads:
    summary.append("### 🕵️ WARNING: Sensitive Files Accessed 🕵️")
    summary.append("| PID | Command | Files Accessed |")
    summary.append("|---|---|---|")
    for read_pid, files in sensitive_reads.items():
        summary.append(f"| {read_pid} | `{format_for_display(processes.get(read_pid))}` | {', '.join(f'`{f}`' for f in files)} |")
    summary.append("\n---\n")

if exfil_net_pids:
    summary.append("### 🌐 WARNING: Unexpected Outbound Connection 🌐")
    summary.append("| PID | Command | Target IPs (Resolved) |")
    summary.append("|---|---|---|")
    for e_pid in exfil_net_pids: 
        targets = ", ".join(f"`{ip}`" for ip in connection_map.get(e_pid, ["Unknown"]))
        summary.append(f"| {e_pid} | `{format_for_display(processes.get(e_pid))}` | {targets} |")
    summary.append("\n---\n")

if staging_pids:
    summary.append("### 📦 CRITICAL: Data Staging / Encryption 📦")
    summary.append("| PID | Command |")
    summary.append("|---|---|")
    for s_pid in staging_pids: summary.append(f"| {s_pid} | `{format_for_display(processes.get(s_pid))}` |")
    summary.append("\n---\n")

summary.extend(["### 🌳 CI/CD Process Execution Context\n", "```text"])

def build_tree(current_pid, depth, is_last):
    indent = "    " * depth
    prefix = "└── " if is_last else "├── "
    if depth == 0: prefix = ""
    
    alert_tags = ""
    if current_pid in privesc_pids: alert_tags += "🚨[PRIVESC] "
    if current_pid in tty_pids: alert_tags += "💀[REVSHELL] "
    if current_pid in env_reads: alert_tags += "☢️[ENV_SCRAPING] "
    if current_pid in mem_reads: alert_tags += "🧠[MEM_SCRAPING] "
    if current_pid in sensitive_reads: alert_tags += "📂[SENSITIVE_READ] "
    if current_pid in staging_pids: alert_tags += "📦[STAGING] "
    if current_pid in exfil_net_pids: alert_tags += "🌐[OUTBOUND_CONN] "
    if current_pid in lotl_pids: alert_tags += "🐍[LOTL_ABUSE] "
    
    cmd = format_for_display(processes.get(current_pid), max_len=150)
    summary.append(f"{indent}{prefix}[{current_pid}] {alert_tags}{cmd}")
    
    children = children_map.get(current_pid, [])
    for i, child_pid in enumerate(children):
        build_tree(child_pid, depth + 1, i == (len(children) - 1))

for root_ppid in sorted(roots):
    for i, child_pid in enumerate(children_map[root_ppid]):
        build_tree(child_pid, 0, i == (len(children_map[root_ppid]) - 1))

summary.append("```\n")

full_summary_text = '\n'.join(summary)

# --- SAVE ARTIFACT LOCALLY ---
try:
    with open(artifact_summary_file, 'w') as f:
        f.write(full_summary_text)
    print(f"✅ Full unredacted summary saved to artifact file: {artifact_summary_file}")
except Exception as e:
    print(f"❌ Failed to save local summary artifact: {e}")

# --- PUSH TO GITHUB STEP SUMMARY (WITH SAFEGUARDS) ---
summary_path = os.environ.get('GITHUB_STEP_SUMMARY')
if summary_path:
    # GitHub limits step summaries to 1024 * 1024 bytes (1MB). We truncate at 950,000 characters to be safe.
    MAX_UI_LENGTH = 950000 
    
    if len(full_summary_text) > MAX_UI_LENGTH:
        truncated_text = full_summary_text[:MAX_UI_LENGTH]
        warning_header = (
            "### ⚠️ SUMMARY TRUNCATED ⚠️\n"
            "**The process tree for this workflow exceeded GitHub's 1MB display limit.** "
            "Please download the `security-baseline` artifact zip and open `security_summary.md` "
            "to view the complete report.\n\n---\n"
        )
        final_ui_text = warning_header + truncated_text + "\n\n```\n\n*(...output truncated...)*"
    else:
        final_ui_text = full_summary_text
        
    with open(summary_path, 'a') as f: 
        f.write(final_ui_text)
else:
    print(full_summary_text)