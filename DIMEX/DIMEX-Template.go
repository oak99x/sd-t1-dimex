package DIMEX

import (
	PP2PLink "SD/PP2PLink"
	"fmt"
	"strings"
	"strconv"
	"os"
	"sync"
)

// ------------------------------------------------------------------------------------
// ------- principais tipos
// ------------------------------------------------------------------------------------

type State int // enumeracao dos estados possiveis de um processo

const (
	noMX State = iota
	wantMX
	inMX
)

type dmxReq int // enumeracao dos estados possiveis de um processo

const (
	ENTER dmxReq = iota
	EXIT
	START_SNAPSHOT
)

type dmxResp struct { // mensagem do módulo DIMEX infrmando que pode acessar - pode ser somente um sinal (vazio)
	// mensagem para aplicacao indicando que pode prosseguir
}

type DIMEX_Module struct {
	Req       chan dmxReq  // canal para receber pedidos da aplicacao (ENTER, EXIT, START_SNAPSHOT)
	Ind       chan dmxResp // canal para informar aplicacao que pode acessar
	addresses []string     // endereco de todos, na mesma ordem
	id        int          // identificador do processo - é o indice no array de enderecos acima
	st        State        // estado deste processo na exclusao mutua distribuida
	waiting   []bool       // processos aguardando tem flag true
	lcl       int          // relogio logico local
	reqTs     int          // timestamp local da ultima requisicao deste processo
	nbrResps  int
	dbg       bool

	Pp2plink *PP2PLink.PP2PLink // acesso aa comunicacao enviar por PP2PLinq.Req  e receber por PP2PLinq.Ind

	// Variáveis para Snapshot
	SnapMu sync.Mutex

	idSnapShot    int64
	snapshots       map[int64]*SnapshotState // Map de snapshots com ID do snapshot como chave
	activeSnapshot  bool                     // Flag indicando se o snapshot está ativo
	errorInjected bool
	markersReceived map[int]bool             // Map para rastrear marcadores recebidos
}


type SnapshotState struct {
	ProcessState State
	Lcl          int
	ReqTs        int
	Waiting      []bool
	NbrResps     int
	Recorded     bool
	ChannelState map[int][]string // Estado dos canais de entrada
	Messages     []string         // Mensagens recebidas durante o snapshot
}


const MARKER = "MARKER"

// ------------------------------------------------------------------------------------
// ------- inicializacao
// ------------------------------------------------------------------------------------

func NewDIMEX(_addresses []string, _id int, _dbg bool) *DIMEX_Module {

	p2p := PP2PLink.NewPP2PLink(_addresses[_id], _dbg)

	dmx := &DIMEX_Module{
		Req: make(chan dmxReq, 1),
		Ind: make(chan dmxResp, 1),

		addresses: _addresses,
		id:        _id,
		st:        noMX,
		waiting:   make([]bool, len(_addresses)),
		lcl:       0,
		reqTs:     0,
		dbg:       _dbg,

		Pp2plink:  p2p,

		idSnapShot:      0,
		snapshots:       make(map[int64]*SnapshotState),
		activeSnapshot:  false,
		errorInjected:   false,
		markersReceived: make(map[int]bool),
	}

	for i := 0; i < len(dmx.waiting); i++ {
		dmx.waiting[i] = false
	}
	dmx.Start()
	dmx.outDbg("Init DIMEX!")
	return dmx
}

// ------------------------------------------------------------------------------------
// ------- nucleo do funcionamento
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) Start() {

	go func() {
		for {
			select {
			case dmxR := <-module.Req: // vindo da  aplicação
				if dmxR == ENTER {
					module.outDbg("app pede mx")
					module.handleUponReqEntry() // ENTRADA DO ALGORITMO

				} else if dmxR == EXIT {
					module.outDbg("app libera mx")
					module.handleUponReqExit() // ENTRADA DO ALGORITMO
				} else if dmxR == START_SNAPSHOT {
					module.outDbg("app solicita snapshot")
					module.startSnapshot()
				}

			case msgOutro := <-module.Pp2plink.Ind: // vindo de outro processo
				//fmt.Printf("dimex recebe da rede: ", msgOutro)
				if strings.Contains(msgOutro.Message, "respOK") {
					module.outDbg("         <<<---- responde! " + msgOutro.Message)
					module.handleUponDeliverRespOk(msgOutro) // ENTRADA DO ALGORITMO

				} else if strings.Contains(msgOutro.Message, "reqEntry") {
					module.outDbg("          <<<---- pede??  " + msgOutro.Message)
					module.handleUponDeliverReqEntry(msgOutro) // ENTRADA DO ALGORITMO
				} else if strings.Contains(msgOutro.Message, MARKER) {
					module.handleMarker(msgOutro)
				}
			}
		}
	}()
}

// ------------------------------------------------------------------------------------
// ------- tratamento de pedidos vindos da aplicacao
// ------- UPON ENTRY
// ------- UPON EXIT
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponReqEntry() {
	module.lcl++
	module.reqTs = module.lcl
	module.nbrResps = 0
	
	for i, addr := range module.addresses {
		if i != module.id {
			module.sendToLink(addr, fmt.Sprintf("reqEntry||%d||%d", module.id, module.reqTs), "     ")
		}
	}

	module.st = wantMX
}

func (module *DIMEX_Module) handleUponReqExit() {
	for i, addr := range module.addresses {
		if module.waiting[i] {
			module.sendToLink(addr, fmt.Sprintf("respOK||%d||%d", module.id, module.lcl), "     ")
			module.waiting[i] = false
		}
	}
	module.st = noMX
}

// ------------------------------------------------------------------------------------
// ------- tratamento de mensagens de outros processos
// ------- UPON respOK
// ------- UPON reqEntry
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponDeliverRespOk(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	parts := strings.Split(msgOutro.Message, "||")
	senderId, _ := strconv.Atoi(parts[1])

	module.nbrResps++
	if module.nbrResps == len(module.addresses)-1 {
		module.st = inMX
		module.Ind <- dmxResp{}
	}

	module.messageInterceptor(senderId, msgOutro.Message)
}

func (module *DIMEX_Module) handleUponDeliverReqEntry(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	parts := strings.Split(msgOutro.Message, "||")
	senderId, _ := strconv.Atoi(parts[1])
	senderTs, _ := strconv.Atoi(parts[2])

	if module.st == noMX || (module.st == wantMX && after(module.id, module.reqTs, senderId, senderTs)) {
		module.sendToLink(module.addresses[senderId], fmt.Sprintf("respOK||%d||%d", module.id, module.lcl), "     ")
	} else {
		module.waiting[senderId] = true
	}

	module.lcl = max(module.lcl, senderTs)

	module.messageInterceptor(senderId, msgOutro.Message)
}

// ------------------------------------------------------------------------------------
// ------- funcoes de ajuda
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) sendToLink(address string, content string, space string) {
	//module.outDbg(space + " ---->>>>   to: " + address + "     msg: " + content)
	module.Pp2plink.Req <- PP2PLink.PP2PLink_Req_Message{
		To:      address,
		Message: content}
}

func before(oneId, oneTs, othId, othTs int) bool {
	if oneTs < othTs {
		return true
	} else if oneTs > othTs {
		return false
	} else {
		return oneId < othId
	}
}

func after(oneId, oneTs, othId, othTs int) bool {
	if oneTs > othTs {
		return true
	} else if oneTs < othTs {
		return false
	} else {
		return oneId > othId
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (module *DIMEX_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . . . . [ DIMEX : " + s + " ]")
	}
}


// ------------------------------------------------------------------------------------
// -------  funcoes Snapshot (Chandy-Lamport)
// ------------------------------------------------------------------------------------

// startSnapshot: iniciador grava estado local imediatamente, escreve seu arquivo e envia MARKER para os outros
func (module *DIMEX_Module) startSnapshot() {
	module.SnapMu.Lock()
	defer module.SnapMu.Unlock()

	if module.activeSnapshot {
		fmt.Println("Snapshot já está em andamento.")
		return
	}

	snapshotId := module.idSnapShot + 1
	module.idSnapShot = snapshotId

	module.outDbg(fmt.Sprintf("Iniciando snapshot com o id: %d", snapshotId))

	// marca como ativo e grava estado local
	module.activeSnapshot = true
	module.recordState(snapshotId)

	// escreve o estado local desse processo no arquivo imediatamente
	module.writeSnapshotToFileLocked(snapshotId)

	// envia marcador para todos os outros processos (não envia para si via rede)
	for i := 0; i < len(module.addresses); i++ {
		if i != module.id {
			module.sendToLink(module.addresses[i], fmt.Sprintf("%s||%d||%d", MARKER, module.id, snapshotId), "     ")
		}
	}
}

// middleware durante o snapshot: grava mensagens recebidas por canal enquanto o snapshot não recebeu o marker daquele canal
func (module *DIMEX_Module) messageInterceptor(senderId int, stringMsg string) {
	module.SnapMu.Lock()
	defer module.SnapMu.Unlock()

	if module.activeSnapshot {
		for _, snapshot := range module.snapshots {
			// se ainda não recebeu marcador do sender, essa mensagem pertence ao estado do canal de entrada senderId
			if !snapshot.Waiting[senderId] {
				// armazena por canal
				snapshot.ChannelState[senderId] = append(snapshot.ChannelState[senderId], stringMsg)
				// também manter lista agregada (opcional)
				snapshot.Messages = append(snapshot.Messages, stringMsg)
			}
		}
	}
}

func (module *DIMEX_Module) handleMarker(msg PP2PLink.PP2PLink_Ind_Message) {
	senderId, snapshotId := parseMarkerMessage(msg.Message)
	if snapshotId == 0 {
		return
	}

	module.SnapMu.Lock()
	defer module.SnapMu.Unlock()

	// atualiza idSnapShot se necessário
	if snapshotId > module.idSnapShot {
		module.idSnapShot = snapshotId
	}

	// Verifica se o snapshot já foi gravado localmente
	if !module.isIdInSnapshotLocked(snapshotId) {
		// primeira vez recebendo MARKER para esse snapshot
		module.activeSnapshot = true
		module.recordState(snapshotId)

		// marca que recebeu do sender (stop recording desse canal)
		if senderId >= 0 && senderId < len(module.addresses) {
			module.snapshots[snapshotId].Waiting[senderId] = true
		}

		module.outDbg(fmt.Sprintf("Primeiro MARKER recebido de %d para snapshot %d. Estado local gravado.", senderId, snapshotId))

		// escreve o arquivo local imediatamente
		module.writeSnapshotToFileLocked(snapshotId)

		// envia MARKER para todos os outros (exceto si mesmo)
		for i := 0; i < len(module.addresses); i++ {
			if i != module.id {
				module.sendToLink(module.addresses[i], fmt.Sprintf("%s||%d||%d", MARKER, module.id, snapshotId), "     ")
			}
		}
	} else {
		// já havia gravado: parar de gravar mensagens do canal sender
		if senderId >= 0 && senderId < len(module.addresses) {
			module.snapshots[snapshotId].Waiting[senderId] = true
		}
		module.outDbg(fmt.Sprintf("Recebido MARKER (subsequente) de %d para snapshot %d. Parando gravação para esse canal.", senderId, snapshotId))
	}

	// verifica se completou (todos os canais de entrada já receberam MARKER)
	if module.allMarkersReceivedLocked(snapshotId) {
		module.completeSnapshotLocked(snapshotId)
	}
}

func (module *DIMEX_Module) completeSnapshotLocked(snapshotId int64) {
	module.outDbg(fmt.Sprintf("Snapshot %d completo.", snapshotId))
	module.activeSnapshot = false
	module.markersReceived = make(map[int]bool) // Limpa para futuros snapshots

	// por enquanto, escrevemos novamente o snapshot final (inclui channel state)
	module.writeSnapshotToFileLocked(snapshotId)
}

func (module *DIMEX_Module) allMarkersReceivedLocked(snapshotId int64) bool {
	snapshot, ok := module.snapshots[snapshotId]
	if !ok {
		return false
	}
	// se existir algum canal que não recebeu marcador, ainda não acabou
	for _, waiting := range snapshot.Waiting {
		if !waiting {
			return false
		}
	}
	return true
}

func (module *DIMEX_Module) recordState(snapshotId int64) {
	// aqui assume-se que SnapMu está adquirido pelo chamador
	// cria snapshot com Waiting do tamanho correto
	waiting := make([]bool, len(module.addresses))
	for i := range waiting {
		waiting[i] = false
	}

	// copia observacao atual de waiting (opcional)
	// observedWaiting := make([]bool, len(module.waiting))
	// copy(observedWaiting, module.waiting)

	snapshot := &SnapshotState{
		ProcessState: module.st,
		Lcl:          module.lcl,
		ReqTs:        module.reqTs,
		Waiting:      waiting,
		NbrResps:     module.nbrResps,
		Recorded:     true,
		ChannelState: make(map[int][]string),
		Messages:     make([]string, 0),
	}
	// marca que ja recebeu marcator do seu proprio canal (auto-marker)
	if module.id >= 0 && module.id < len(waiting) {
		snapshot.Waiting[module.id] = true
	}
	module.snapshots[snapshotId] = snapshot
}

func (module *DIMEX_Module) isIdInSnapshot(id int64) bool {
	module.SnapMu.Lock()
	defer module.SnapMu.Unlock()
	return module.isIdInSnapshotLocked(id)
}

func (module *DIMEX_Module) isIdInSnapshotLocked(id int64) bool {
	if module.snapshots != nil {
		if _, exists := module.snapshots[id]; exists {
			return true
		}
	}
	return false
}

func parseMarkerMessage(message string) (int, int64) {
	// message format: "MARKER||senderId||snapshotId"
	// debug print
	// fmt.Println("Mensagem recebida:", message)
	parts := strings.Split(message, "||")

	if len(parts) != 3 {
		// fmt.Println("Formato inválido")
		return 0, 0
	}

	senderId, err1 := strconv.Atoi(parts[1])
	snapshotId, err2 := strconv.ParseInt(parts[2], 10, 64)

	if err1 != nil || err2 != nil {
		// fmt.Println("Erro ao analisar a mensagem")
		return 0, 0
	}
	// fmt.Println("ID do snapshot:", snapshotId)
	// fmt.Println("ID do sender:", senderId)
	return senderId, snapshotId
}

func (module *DIMEX_Module) writeSnapshotToFile(snapshotId int64) {
	module.SnapMu.Lock()
	defer module.SnapMu.Unlock()
	module.writeSnapshotToFileLocked(snapshotId)
}

func (module *DIMEX_Module) writeSnapshotToFileLocked(snapshotId int64) {
	filename := fmt.Sprintf("./snapshots/process_%d.txt", module.id)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// fmt.Println("Erro ao abrir/criar arquivo de snapshot:", err)
		return
	}
	defer file.Close()
	// fmt.Println("snapshot id ", snapshotId)
	snapshot, ok := module.snapshots[snapshotId]
	if !ok {
		return
	}
	file.WriteString(fmt.Sprintf("Snapshot %d\n", snapshotId))
	file.WriteString(fmt.Sprintf("Estado: %d\n", snapshot.ProcessState))
	file.WriteString(fmt.Sprintf("Relógio Lógico: %d\n", snapshot.Lcl))
	file.WriteString(fmt.Sprintf("Timestamp de Requisição: %d\n", snapshot.ReqTs))
	file.WriteString(fmt.Sprintf("Receive resps: %v\n", snapshot.Waiting))
	file.WriteString(fmt.Sprintf("Waiting: %v\n", module.waiting))
	file.WriteString(fmt.Sprintf("NbrResps: %d\n", snapshot.NbrResps))
	file.WriteString(fmt.Sprintf("Mensagens:\n"))
	
	// for _, msg := range snapshot.Messages {
	// 	file.WriteString(fmt.Sprintf("%s\n", msg))
	// }

	for _, msgs := range snapshot.ChannelState {
        if len(msgs) > 0 {
			for _, m := range msgs {
				file.WriteString(fmt.Sprintf("%s\n", m))
			}
		}
    }

	file.WriteString("\n")
}