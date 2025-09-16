package DIMEX

import (
	PP2PLink "SD/PP2PLink"
	"fmt"
	"strings"
	"strconv"
	"os"
)

// ------------------------------------------------------------------------------------
// ------- principais tipos
// ------------------------------------------------------------------------------------

type State int // enumeracao dos estados possiveis de um processo

const (
	noMX State = iota // nao quer entrar na seção critica
	wantMX //quer entrar na seção critica
	inMX //esta na seção critica
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
	activeSnapshot      bool
    currentSnapshotId   int64
    nextSnapshotId      int64
    currentSnapshotState *SnapshotState
    errorInjected bool
}


type SnapshotState struct {
	ProcessState    State
    Lcl             int
    ReqTs           int
    NbrResps        int
    Recorded        bool
    ChannelState    map[int][]string // Estado dos canais de entrada (chave: senderId, valor: mensagens)
    WaitingForMarker map[int]bool // Rastreia os marcadores que ainda faltam
	WaitingState     []bool // Novo campo para capturar o estado de 'waiting'
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

		// Inicializa as variáveis do snapshot
        activeSnapshot:      false,
        currentSnapshotId:   0, // Nenhum snapshot está ativo no início
        nextSnapshotId:      1, // O primeiro snapshot terá ID 1
        currentSnapshotState: nil,
        errorInjected: false,
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

	module.messageInterceptor(senderId, msgOutro.Message)

	module.nbrResps++
	if module.nbrResps == len(module.addresses)-1 {
		module.st = inMX
		module.Ind <- dmxResp{}
	}
}

func (module *DIMEX_Module) handleUponDeliverReqEntry(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	parts := strings.Split(msgOutro.Message, "||")
	senderId, _ := strconv.Atoi(parts[1])
	senderTs, _ := strconv.Atoi(parts[2])

	module.messageInterceptor(senderId, msgOutro.Message)

	if module.st == noMX || (module.st == wantMX && after(module.id, module.reqTs, senderId, senderTs)) {
		module.sendToLink(module.addresses[senderId], fmt.Sprintf("respOK||%d||%d", module.id, module.lcl), "     ")
	} else {
		module.waiting[senderId] = true
	}

	module.lcl = max(module.lcl, senderTs)
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
// ------- funcoes Snapshot
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) startSnapshot() {
    if module.activeSnapshot {
        fmt.Println("Snapshot já está em andamento.")
        return
    }

    snapshotId := module.nextSnapshotId

    // Cria e armazena o novo snapshot
    snapshotWaitingState := make([]bool, len(module.waiting))
    copy(snapshotWaitingState, module.waiting)

    module.currentSnapshotState = &SnapshotState{
        ProcessState:    module.st,
        Lcl:             module.lcl,
        ReqTs:           module.reqTs,
        NbrResps:        module.nbrResps,
        Recorded:        true,
        ChannelState:    make(map[int][]string),
        WaitingForMarker: make(map[int]bool),
        WaitingState:    snapshotWaitingState, // Captura o estado de 'waiting' no momento do snapshot
    }

    // Seta o próprio marcador como recebido
    module.currentSnapshotState.WaitingForMarker[module.id] = true
    module.activeSnapshot = true
    module.currentSnapshotId = snapshotId
    module.nextSnapshotId++
    
    fmt.Println("Iniciando snapshot no processo", module.id)

    // Envia marcadores para todos os processos
    for i := 0; i < len(module.addresses); i++ {
        if i != module.id {
            module.sendToLink(module.addresses[i], fmt.Sprintf("%s||%d||%d", MARKER, module.id, snapshotId), "     ")
        }
    }
}

// middleware durante o snapshot
func (module *DIMEX_Module) messageInterceptor(senderId int, stringMsg string) {
	// Verifica se há um snapshot ativo
    if module.activeSnapshot {
        snapshot := module.currentSnapshotState
        
        // Se ainda não recebeu o marcador deste remetente, grave a mensagem.
        if !snapshot.WaitingForMarker[senderId] {
            // Adiciona a mensagem à lista do canal correspondente
            snapshot.ChannelState[senderId] = append(snapshot.ChannelState[senderId], stringMsg)
        }
    }
}
func (module *DIMEX_Module) handleMarker(msg PP2PLink.PP2PLink_Ind_Message) {
    senderId, snapshotId := parseMarkerMessage(msg.Message)

    if !module.activeSnapshot {
        fmt.Printf("Processo %d recebeu o primeiro marcador (id %d). Iniciando snapshot.\n", module.id, snapshotId)

        snapshotWaitingState := make([]bool, len(module.waiting))
        copy(snapshotWaitingState, module.waiting)

        module.currentSnapshotState = &SnapshotState{
            ProcessState:     module.st,
            Lcl:              module.lcl,
            ReqTs:            module.reqTs,
            NbrResps:         module.nbrResps,
            Recorded:         true,
            ChannelState:     make(map[int][]string),
            WaitingForMarker: make(map[int]bool),
            WaitingState:     snapshotWaitingState, // Captura o estado de 'waiting'
        }
        
        module.currentSnapshotState.WaitingForMarker[module.id] = true
        module.activeSnapshot = true
        module.currentSnapshotId = snapshotId
        
        for i := 0; i < len(module.addresses); i++ {
            if i != module.id {
                module.sendToLink(module.addresses[i], fmt.Sprintf("%s||%d||%d", MARKER, module.id, snapshotId), "     ")
            }
        }
    } else if module.currentSnapshotId != snapshotId {
        fmt.Printf("Processo %d ignorando marcador (id %d) de outro snapshot. Atual ativo: %d.\n", module.id, snapshotId, module.currentSnapshotId)
        return
    }

    module.currentSnapshotState.WaitingForMarker[senderId] = true

    if module.allMarkersReceived() {
        module.completeSnapshot()
    }
}

func (module *DIMEX_Module) completeSnapshot() {
	module.outDbg(fmt.Sprintf("Snapshot %d completo. Escrevendo para o arquivo...", module.currentSnapshotId))
    module.writeSnapshotToFile(module.currentSnapshotId)
    module.activeSnapshot = false // Limpa a flag de snapshot ativo
    module.currentSnapshotId = 0
    module.currentSnapshotState = nil // Limpa o ponteiro para o snapshot
}

func (module *DIMEX_Module) allMarkersReceived() bool {
	// A condição de conclusão é receber marcadores de todos os N-1 outros processos
    return len(module.currentSnapshotState.WaitingForMarker) == len(module.addresses)
}


func parseMarkerMessage(message string) (int, int64) {
	fmt.Println("Mensagem recebida:", message)
	parts := strings.Split(message, "||")

	if len(parts) != 3 {
		fmt.Println("Formato inválido")
		return 0, 0
	}

	senderId, err1 := strconv.Atoi(parts[1])
	snapshotId, err2 := strconv.ParseInt(parts[2], 10, 64)

	if err1 != nil || err2 != nil {
		fmt.Println("Erro ao analisar a mensagem")
		return 0, 0
	}
	fmt.Println("ID do snapshot:", snapshotId)
	fmt.Println("ID do sender:", senderId)
	return senderId, snapshotId
}
func (module *DIMEX_Module) writeSnapshotToFile(snapshotId int64) {
    filename := fmt.Sprintf("./snapshots/process_%d.txt", module.id)
    file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return
    }
    defer file.Close()
    snapshot := module.currentSnapshotState
    
    file.WriteString(fmt.Sprintf("Snapshot %d\n", snapshotId))
    file.WriteString(fmt.Sprintf("Estado: %d\n", snapshot.ProcessState))
    file.WriteString(fmt.Sprintf("Relógio Lógico: %d\n", snapshot.Lcl))
    file.WriteString(fmt.Sprintf("Timestamp de Requisição: %d\n", snapshot.ReqTs))
    file.WriteString(fmt.Sprintf("Receive resps: %v\n", module.currentSnapshotState.WaitingForMarker))
    file.WriteString(fmt.Sprintf("Waiting: %v\n", snapshot.WaitingState))
    file.WriteString(fmt.Sprintf("NbrResps: %d\n", snapshot.NbrResps))
    file.WriteString(fmt.Sprintf("Mensagens:\n"))

    // Adiciona as mensagens gravadas por canal ao arquivo
    for senderID, messages := range snapshot.ChannelState {
        file.WriteString(fmt.Sprintf("  Canal de %d:\n", senderID))
        for _, msg := range messages {
            file.WriteString(fmt.Sprintf("    %s\n", msg))
        }
    }

    file.WriteString("\n")
}