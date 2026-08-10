## Goy Node

**Relay Nostr descentralizado com mesh automática e distribuição de dados, da The Goy Company.**

### O que é

O Goy Node é um binário único em Rust que combina um relay Nostr (strfry) com um mesh agent inteligente. Quando instalado, entra automaticamente numa VPN exclusiva da plataforma (`vpn.goyco.xyz`), descobre outros nós Goy via registry central, e começa a sincronizar eventos — sem configuração manual.

Os dados são distribuídos entre os nós da mesh usando replicação N-of-M com seleção determinística (HRW hashing), garantindo redundância e disponibilidade mesmo com falhas de nós individuais.
