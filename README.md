# PanDA Simulator

Simulatorja nadzornega strežnika PanDA in pripadajočega računskega omrežja. 

### Povzetek projekta

Simulator nudi analizo računske
infrastrukture in preizkušanje novih algoritmov za kar se da optimalno porazdelitev računskih bremen, kar lahko v prihodnje pripomore k boljši rabi že postavljene in načrtovanju nove
strojne opreme.

Na podlagi zgodovine izvajanja obstoječih računskih poslov smo generirali umetne računske naloge in preverili ustreznost simulacije z obstoječim razvrščevalnim algoritmom
simulirali njihovo obdelavo.
Simulator razvrščanja računskih poslov tako služi kot orodje za novo preizkušanje idej, ki si jih na dejanski opremi ne moremo privoščiti in omogoča izboljšave pri
delitvi računskih bremen na svetovnih superračunalnikih.

### Uporaba

Simulator nadzornega strežnika PanDA se nahaja v datoteki *main.py*, skupaj z računskim omrežjem *network.py*. Struktura omrežja in simulacijske naloge so podane v datotekah *.json*.
