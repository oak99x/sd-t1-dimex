import os
import re
from collections import defaultdict

SNAPSHOT_DIR = './snapshots'
N = 3
OUTPUT_FILE = './snapshot_analysis.txt'

def read_snapshots():
    snapshots = defaultdict(dict)
    pattern = re.compile(r'process_(\d+)\.txt')

    for filename in os.listdir(SNAPSHOT_DIR):
        match = pattern.match(filename)
        if match:
            processId = int(match.group(1))
            filepath = os.path.join(SNAPSHOT_DIR, filename)
            with open(filepath, 'r') as f:
                snapshotData = None
                snapshotId = None
                messages_section = False
                for line in f:
                    line = line.strip()
                    if 'Snapshot' in line:
                        if snapshotData:
                            snapshots[snapshotId][processId] = snapshotData
                        snapshotId = int(line.split(' ')[1])
                        snapshotData = {'ProcessId': processId, 'SnapshotId': snapshotId, 'Messages': [], 'NbrResps': 0, 'Waiting': []}
                        messages_section = False
                    elif 'Estado:' in line:
                        snapshotData['State'] = int(line.split(': ')[1])
                    elif 'Relógio Lógico:' in line:
                        snapshotData['Lcl'] = int(line.split(': ')[1])
                    elif 'Timestamp de Requisição:' in line:
                        snapshotData['ReqTs'] = int(line.split(': ')[1])
                    elif 'Waiting:' in line:
                        snapshotData['Waiting'] = parse_waiting_list(line.split(': ')[1])
                    elif 'NbrResps:' in line:
                        snapshotData['NbrResps'] = int(line.split(': ')[1])
                    elif 'Mensagens:' in line:
                        messages_section = True
                        continue
                    elif messages_section and line:
                        # Captura todas as mensagens na seção
                        snapshotData['Messages'].append(line)
                if snapshotData:
                    snapshots[snapshotId][processId] = snapshotData
    return snapshots

def parse_waiting_list(waiting_str):
    waiting_str = waiting_str.strip('[]')
    waiting_values = re.findall(r'\btrue\b|\bfalse\b', waiting_str)
    return [value.lower() == 'true' for value in waiting_values]

def check_invariant_1(snapshot):
    # Invariante 1: No máximo um processo na seção crítica
    in_mx_count = sum(1 for s in snapshot.values() if s['State'] == 2)
    return in_mx_count <= 1

def check_invariant_2(snapshot):
    # Invariante 2: Se todos os processos estão em noMX (não querem SC), então todos os waitings são falsos e não deve haver mensagens
    all_no_mx = all(s['State'] == 0 for s in snapshot.values())
    if all_no_mx:
        for s in snapshot.values():
            if s['Messages'] or any(s['Waiting']):
                return False
        return True
    return True

def check_invariant_3(snapshot):
    # Invariante 3: Se q está marcado como waiting em p, então p está em inMX ou wantMX
    for p_data in snapshot.values():
        p_state = p_data['State']
        p_waiting = p_data['Waiting']
        for q_id, waiting in enumerate(p_waiting):
            if waiting:
                if p_state not in [1, 2]:
                    return False
    return True


def check_invariant_4(snapshot):
    # Invariante 4 (Lamport): Um processo em 'wantMX' só deve ter N-1 respostas se sua requisição for a mais antiga.
    for p_data in snapshot.values():
        if p_data['State'] == 1:
            req_ts = p_data.get('ReqTs', 0)
            p_id = p_data.get('ProcessId')
            
            # Conta mensagens de 'respOK' em trânsito
            resp_ok_in_transit = sum(1 for msg in p_data.get('Messages', []) if 'respOK' in msg)
            
            # A soma de respostas recebidas e em trânsito
            total_permissions = p_data.get('NbrResps', 0) + resp_ok_in_transit
            
            # Verifica se ele tem permissão para entrar na SC
            if total_permissions == N - 1:
                # Se tem todas as permissões, sua requisição deve ser a mais antiga
                is_oldest_req = True
                for other_data in snapshot.values():
                    other_req_ts = other_data.get('ReqTs', 0)
                    other_id = other_data.get('ProcessId')
                    
                    # Compara com todas as requisições ativas
                    if other_data['State'] == 1 and (other_req_ts < req_ts or (other_req_ts == req_ts and other_id < p_id)):
                        is_oldest_req = False
                        break
                
                # Se não é a requisição mais antiga, a invariante é violada
                if not is_oldest_req:
                    print(f"Process {p_data['ProcessId']} (Snapshot {p_data['SnapshotId']}) - VIOLAÇÃO: Tem permissões suficientes, mas sua requisição não é a mais antiga.")
                    return False
    
    # Se o processo tem requisição, mas não tem permissão suficiente, é um estado válido
    return True

def analyze_snapshots(snapshots):
    with open(OUTPUT_FILE, 'w') as f:
        for snapshotId, snapshot in sorted(snapshots.items()):
            result = f"\nAnalisando Snapshot {snapshotId}:\n"
            inv1 = check_invariant_1(snapshot)
            inv2 = check_invariant_2(snapshot)
            inv3 = check_invariant_3(snapshot)
            inv4 = check_invariant_4(snapshot)

            result += " - Invariante 1 mantida\n" if inv1 else " - Invariante 1 VIOLADA\n"
            result += " - Invariante 2 mantida\n" if inv2 else " - Invariante 2 VIOLADA\n"
            result += " - Invariante 3 mantida\n" if inv3 else " - Invariante 3 VIOLADA\n"
            result += " - Invariante 4 mantida\n" if inv4 else " - Invariante 4 VIOLADA\n"
            print(result)
            f.write(result)

if __name__ == "__main__":
    snapshots = read_snapshots()
    analyze_snapshots(snapshots)