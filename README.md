# T1 - DiMEx + Snapshot
## Sistemas Distribuídos

## Autores
- Gabriel Moszkowicz
- Lucas Lorenzi da Silva
- Mateus Freitas
- Rauf Rodrigues

#### Parte 1 - Exclusão Mútua Distribuída
Implementação de um algoritmo de exclusão mútua distribuída utilizando o DiMEx.

#### Parte 2 - Snapshot Global (Chandy-Lamport)
Integração do algoritmo de snapshot ao DiMEx, baseado no algoritmo de Chandy-Lamport discutido em aula.




## Como executar:
Abra três terminais diferentes e execute os seguintes comandos (um em cada terminal):

```bash
go run useDIMEX-f.go 0 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002
go run useDIMEX-f.go 1 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002
go run useDIMEX-f.go 2 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002
```


#### Observações

- Foi definido um tempo de início de 15 segundos, permitindo que os três comandos sejam executados tranquilamente.

- Após 15 segundos, o sistema starta a troca de mensagens e logo inicia snapshots automáticos a cada 2 segundos (loop de snapshots).