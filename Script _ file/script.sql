############################################################################
################      Script per progetto BDSI 2019/20     #################
############################################################################
#
# GRUPPO FORMATO DA:
#
# Matricola: 7013974      Cognome: Menichetti	       Nome: Matteo    
# Matricola: 7013975     Cognome: Miniati	       Nome: Federico    
#
############################################################################
drop database if exists dbProgetto;
create database if not exists dbProgetto;
use dbProgetto;

CREATE TABLE IF NOT EXISTS Squadra (
    nome CHAR(20) PRIMARY KEY,
    sede CHAR(20),
    colori_sociali TEXT,
    stadio TEXT,
    punti INT(3) DEFAULT 0
)  ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS Partita (
    codice INT(2) AUTO_INCREMENT PRIMARY KEY,
    data DATE,
    squadra_casa CHAR(20),
    squadra_ospite CHAR(20),
    goal_casa int1 DEFAULT 0,
    goal_ospite int1 DEFAULT 0,
    numero_giornata int2,
    CHECK (goal_casa >= 0 and goal_ospite >= 0),
    FOREIGN KEY (squadra_casa)
        REFERENCES Squadra (nome),
    FOREIGN KEY (squadra_ospite)
        REFERENCES Squadra (nome)
)  ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS Giocatore (
    cf CHAR(20) PRIMARY KEY,
    squadra CHAR(20),
    nome CHAR(20),
    cognome CHAR(20),
    data_nascita DATE,
    stipendio INT(10) DEFAULT 0,
    ruolo ENUM('P', 'D', 'A', 'C'),
    CHECK (stipendio > 0),
    FOREIGN KEY (squadra)
        REFERENCES Squadra(nome)
	
)  ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS StoricoGoal (
    giocatore CHAR(20),
    partita INT(2),
    PRIMARY KEY (giocatore , partita),
    numero_goal int2,
    FOREIGN KEY (giocatore)
        REFERENCES Giocatore (cf),
    FOREIGN KEY (partita)
        REFERENCES Partita (codice)
)  ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS Allenatore (
    cf CHAR(20) PRIMARY KEY,
    squadra CHAR(20),
    nome CHAR(20),
    cognome CHAR(20),
    data_nascita DATE,
    stipendio INT(10),
    CHECK (stipendio > 0),
    FOREIGN KEY (squadra)
        REFERENCES Squadra (nome)
)  ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS Transazione (
    codice int3 AUTO_INCREMENT PRIMARY KEY,
    giocatore CHAR(20),
    squadra_acquirente CHAR(20),
    data DATE,
    data_fine DATE,
    tipo enum  ('P','V'),
    importo INT(10) default 0,
    FOREIGN KEY (giocatore)
        REFERENCES Giocatore (cf),
    FOREIGN KEY (squadra_acquirente)
        REFERENCES Squadra (nome)
)  ENGINE=INNODB;

#############################################################################
################  Ulteriori vincoli tramite viste e/o trigger ################
#############################################################################

delimiter &&
create trigger PartitaTrigger
before insert on Partita
for each row
begin

if (new.squadra_casa=new.squadra_ospite)then
			SIGNAL SQLSTATE VALUE '45000'
			SET MESSAGE_TEXT = 'Squadra casa = Squadra ospite';
  end if;
if (new.squadra_casa in (select squadra_casa from Partita where (numero_giornata=new.numero_giornata)))then 
		SIGNAL SQLSTATE VALUE '45000'
			SET MESSAGE_TEXT = 'Squadra di casa ha già giocato in giornata';
       end if;     
if(new.squadra_ospite in (select squadra_ospite from Partita where (numero_giornata=new.numero_giornata))) then
			SIGNAL SQLSTATE VALUE '45000'
			SET MESSAGE_TEXT = 'Squadra casa ha già giocato in giornata';
 end if;
end &&

create trigger ControlloTransazione
before insert on Transazione /* se una vendita / prestito di un giocatore viene effettuato prima della scadenza del precedente prestito
								è necessario aggiornare la data di fine del prestito che viene interrotto */
for each row
begin 
declare data date;
declare codice int3;
if ((select count(*) from Transazione where tipo='P') > 0)then
 set data = (select data_fine from Transazione where codice = (select max(codice) from Transazione where new.giocatore = giocatore and tipo = 'P' ));
 set codice = (select codice from Transazione where codice = (select max(codice) from Transazione where new.giocatore = giocatore and tipo = 'P' ));
if(data > new.data)then 
update Transazione set data_fine=new.data where Transazione.codice=codice;
end if;
end if ;

end&&

create trigger AggiornamentoGiocatore
after insert on Transazione /* se viene effettuata una vendita è necessario aggiornare la squadra di appartenenza del giocatore*/
for each row 
begin
if(new.tipo='V')then 
update Giocatore set squadra = new.squadra_acquirente where cf=new.giocatore;
end if;
end&&

create trigger ControlloGoal
before insert on StoricoGoal /* viene verificato che un giocatore giochi per una squadra in quella giornata. Verificando anche tramite i prestiti
								dove gioca in quanto un giocatore in prestito ad una Squadra B, anche se appartiene ad una squadra A,
                                non può segnare per la squadra A */
for each row
begin 
	declare squadra_giocatore, squadra_casa, squadra_ospite char(20);
    declare msg text;
    declare max_count int3;
    if(squadragiocatore(new.giocatore) != NULL)then 
	set squadra_giocatore = squadragiocatore(new.giocatore);
	else set squadra_giocatore = (select Giocatore.squadra from Giocatore where cf = new.giocatore limit 1);
	end if;
    set squadra_ospite = (select Partita.squadra_ospite from Partita where codice=new.partita limit 1);
    set squadra_casa = (select Partita.squadra_casa from Partita where codice=new.partita limit 1);
    set msg = concat(new.giocatore,' non gioca in nessuna delle squadre che giocano: ', squadra_casa,' - ',squadra_ospite);
    if ((squadra_giocatore != squadra_casa and squadra_giocatore != squadra_ospite))then
    SIGNAL SQLSTATE VALUE '45000' SET MESSAGE_TEXT = msg;
	end if;
end &&

create trigger AggiornamentoGoal /* aggiornamento dei goal di una squadra, in una determinata partita, a seconda della squadra del giocatore che
									ha segnato*/
after insert on StoricoGoal
for each row
begin 
declare squadra char(20);
if(squadragiocatore(new.giocatore) != NULL)then 
set squadra = squadragiocatore(new.giocatore);
else set squadra = (select Giocatore.squadra from Giocatore where cf = new.giocatore limit 1);
end if;
	if(squadra in (select Partita.squadra_casa from Partita where codice = new.partita))then 
		update Partita set goal_casa = goal_casa + new.numero_goal where codice = new.partita;
	else if(squadra in (select Partita.squadra_ospite from Partita where codice = new.partita))then
		update Partita set goal_ospite = goal_ospite + new.numero_goal where codice = new.partita;
    end if;
	end if;
end &&

############################################################################
################          Procedure e funzioni             #################
############################################################################
# Per la consegna del 25 maggio non sono richiesti handler e cursori.

create procedure AggiornamentoClassifica()
begin
declare gc, go int default 0; -- goal ospiti, goal 
declare sc , so char(20);
declare cursore cursor for (select squadra_casa , squadra_ospite, goal_casa, goal_ospite from Partita);
DECLARE exit HANDLER FOR NOT FOUND close cursore; 
open cursore;
loop 
	fetch cursore into sc, so, gc, go;
    if(gc>go) then call CalcoloRisultato(sc, 3); 
	else if (gc = go) then begin call CalcoloRisultato(sc, 1);call CalcoloRisultato(so, 1);end;
    else if(gc<go) then call CalcoloRisultato(so, 3);
    end if;
    end if;
    end if;
 end loop;
end&&

create procedure CalcoloRisultato(in squadra char(20), in i int3)
begin
update Squadra set punti = punti + i where nome = squadra;
end&&


CREATE FUNCTION squadragiocatore (cf_giocatore char(20)) 
RETURNS char(20)
begin 

declare squadra char(20);

if((select max(codice) from Transazione where cf_giocatore = giocatore and tipo = 'P') > (select max(codice) from Transazione where cf_giocatore = giocatore and tipo = 'V') )then
	return (select squadra_acquirente from Transazione where codice = 
	(select max(codice) from Transazione where cf_giocatore = giocatore and tipo = 'P'));
    
else return (select squadra_acquirente from Transazione where codice = 
	(select max(codice) from Transazione where cf_giocatore = giocatore and tipo = 'V'));
end if;

end&&

delimiter ;

############################################################################
################  Creazione istanza: popolamento database  #################
############################################################################
/*load data local infile 'C:/ProgramData/MySQL/MySQL Server 5.6/Uploads/Giocatori.txt' into table Giocatore 
fields terminated by ','
lines terminated by '\n';

load data local infile 'C:/ProgramData/MySQL/MySQL Server 5.6/Uploads/Transazioni.txt' into table Transazione
fields terminated by ','
lines terminated by '\n'
(giocatore, squadra_acquirente,data,tipo,importo);

load data local infile 'C:/ProgramData/MySQL/MySQL Server 5.6/Uploads/TransazioniPrestiti.txt' into table Transazione
fields terminated by ','
lines terminated by '\n'
(giocatore, squadra_acquirente,data,data_fine,tipo);*/

insert into Squadra values 
('Juventus', 'Torino', 'Bianco_Nero','Juventus Stadium',0),
('Napoli', 'Napoli', 'Azzurro', 'San Paolo',0),
('Atalanta', 'Bergamo', 'Blu_Nero', 'Gewiss Stadium',0),
('Inter', 'Milano', 'Blu_Nero', 'San Siro',0),
('Milan', 'Milano', 'Rosso_Nero', 'San Siro',0),
('Empoli', 'Empoli', 'Bianco_Azzurro', 'Carlo Castellani',0);

insert into Giocatore values 
('MR-1','Juventus', 'Mario', 'Rossi', '1900-10-01', 100000,'A'),
('FR-1','Juventus', 'Franco', 'Rossi', '1900-10-01', 100000,'C'),
('DR-1','Juventus', 'Dario', 'Rossi', '1900-10-01', 100000,'D'),
('SR-1','Juventus', 'Sandro', 'Rossi', '1900-10-01', 100000,'P'),
('MR-2','Napoli', 'Mario', 'Rossi', '1900-10-01', 100000,'A'),
('FR-2','Napoli', 'Franco', 'Rossi', '1900-10-01', 100000,'C'),
('DR-2','Napoli', 'Dario', 'Rossi', '1900-10-01', 100000,'D'),
('SR-2','Napoli', 'Sandro', 'Rossi', '1900-10-01', 100000,'P'),
('MR-3','Atalanta', 'Mario', 'Rossi', '1900-10-01', 100000,'A'),
('FR-3','Atalanta', 'Franco', 'Rossi', '1900-10-01', 100000,'C'),
('DR-3','Atalanta', 'Dario', 'Rossi', '1900-10-01', 100000,'D'),
('SR-3','Atalanta', 'Sandro', 'Rossi', '1900-10-01', 100000,'P'),
('MR-4','Inter', 'Mario', 'Rossi', '1900-10-01', 100000,'A'),
('FR-4','Inter', 'Franco', 'Rossi', '1900-10-01', 100000,'C'),
('DR-4','Inter', 'Dario', 'Rossi', '1900-10-01', 100000,'D'),
('SR-4','Inter', 'Sandro', 'Rossi', '1900-10-01', 100000,'P'),
('MR-5','Milan', 'Mario', 'Rossi', '1900-10-01', 100000,'A'),
('FR-5','Milan', 'Franco', 'Rossi', '1900-10-01', 100000,'C'),
('DR-5','Milan', 'Dario', 'Rossi', '1900-10-01', 100000,'D'),
('SR-5','Milan', 'Sandro', 'Rossi', '1900-10-01', 100000,'P'),
('MR-6','Empoli', 'Mario', 'Rossi', '1900-10-01', 100000,'A'),
('FR-6','Empoli', 'Franco', 'Rossi', '1900-10-01', 100000,'C'),
('DR-6','Empoli', 'Dario', 'Rossi', '1900-10-01', 100000,'D'),
('SR-6','Empoli', 'Sandro', 'Rossi', '1900-10-01', 100000,'P');

-- prestiti
 insert into Transazione (giocatore, squadra_acquirente,data,data_fine,tipo) values
('MR-2', 'Juventus', '2018-01-02', '2018-07-02','P'),
('MR-1', 'Empoli', '2018-01-02', '2018-02-20','P'),
('DR-2', 'Atalanta', '2018-01-02', '2018-07-02','P'),
('DR-2', 'Inter', '2018-07-03', '2019-01-02','P'),
('DR-3', 'Napoli', '2018-01-02', '2019-02-20','P'),
('FR-2', 'Milan', '2018-01-02', '2018-07-02','P'),
('MR-5', 'Inter', '2018-01-02', '2019-02-20','P');

-- vendite
insert into Transazione (giocatore, squadra_acquirente,data,tipo,importo) values
('DR-2', 'Empoli', '2018-08-12','V', 1200000),
('DR-3', 'Juventus', '2018-01-15', 'V', 30000000),
('FR-4', 'Atalanta', '2018-08-20','V', 200000),
('SR-5','Inter','2018-01-15', 'V', 25000000),
('MR-6','Milan','2018-01-10','V', 300000000),
('MR-5', 'Atalanta',  '2018-02-20','V', 2900000);

insert into Partita(data, squadra_casa, squadra_ospite, numero_giornata) values
('2018-08-18','Napoli','Juventus',1), -- ('DR-3', 1, 2),('FR-1', 1, 3),('FR-2', 1 , 2),
('2018-08-18','Atalanta','Inter',1),
('2018-08-19','Milan','Empoli',1), -- ('MR-1', 3, 1),('FR-3', 3, 1),
('2018-08-25','Juventus','Napoli',2),
('2018-08-25','Inter','Atalanta',2),-- ('MR-5', 5, 2),
('2018-08-26','Empoli','Milan', 2), -- ('DR-2', 6, 3),
('2018-09-01', 'Empoli', 'Juventus', 3),-- ('DR-1', 7, 2), ('FR-6', 7, 1),
('2018-09-01', 'Napoli', 'Inter', 3), -- ('SR-2', 8, 1),
('2018-09-02', 'Atalanta', 'Milan',3); -- ('MR-4', 9, 10);

insert into StoricoGoal (giocatore, partita, numero_goal) values
('DR-3', 1, 2),
('FR-1', 1, 3),
('FR-2', 1 , 2),
('FR-6', 3, 1),
('MR-6', 3, 1),
('MR-5', 5, 2),
('DR-2', 6, 3),
('DR-1', 7, 2),
('FR-6', 7, 1),
('SR-2', 8, 1),
('MR-3', 9, 10);

/*((select giocatore, squadra_acquirente as squadra from Transazione where codice in(select max(codice) from Transazione where tipo='P' group by giocatore));
create table attuale_squadra(giocatore  char(20), squadra char(20));
insert into attuale_squadra (select giocatore, squadra_acquirente as squadra from Transazione where codice in(select max(codice) from Transazione where tipo='P' group by giocatore));
insert into attuale_squadra select cf, squadra from Giocatore where cf not in (select giocatore as cf from Transazione where tipo='P' group by giocatore having max(codice));

select * from attuale_squadra order by squadra;*/



call AggiornamentoClassifica();

select * from Squadra order by Punti desc;


############################################################################
################ 				 Interrogazioni   		   #################
############################################################################
# Possibilmente di vario tipo:  selezioni, proiezioni, join, con raggruppamento, 
# annidate, con funzioni per il controllo del flusso.
/*
Ricerca delle transazioni di una squadra di un determinato tipo;*/
create procedure RicercaTransazioni(in squadra char(20), in tipo enum('V','P'))
select * from Transazione T where T.tipo = tipo and squadra in (select nome from squadra where nome = squadra);
/*Classifica scambi di maggior valore;*/
select * from transazione order by importo desc;
/*Ricerca del giocatore più pagato;*/
select distinct nome,cognome from giocatore where stipendio = (select max(stipendio) from giocatore);
/*Classifica dei giocatori in base ai goal effettuati;*/
select * from giocatore join storicogoal on cf = giocatore;
/*Giocatori acquistati dall'Empoli;*/
select * from giocatore where cf in (select giocatore from transazione where squadra_acquirente = 'Empoli' and tipo = 'V');
/*Classifica generale delle squadre;*/
select nome,punti from squadra order by punti desc;
/*Giocatori che hanno segnato in Napoli - Juventus;*/
select nome,cognome from Giocatore join storicogoal on cf=giocatore 
where partita in (select codice from partita where squadra_casa = 'Napoli' and squadra_ospite = 'Juventus');
/*Elenco partite della prima giornata che si sono disputate il 19-08-2018;*/
select * from partita where numero_giornata = 1 and data = '2018-08-19';
/*Capocannonieri di ogni squadra;*/
select sum(numero_goal) as goal ,giocatore from storicogoal group by giocatore order by  goal desc;
/*Elenco delle squadre attuali dei giocatori.*/
select squadra_acquirente as squadra, giocatore as cf 
from Transazione where tipo='P' and data_fine>'2018-09-02' 
group by giocatore, codice having max(codice)
union
select squadra, cf 
from Giocatore
where cf not in 
(select giocatore as cf from Transazione where tipo='P' and data_fine>'2018-09-02' group by giocatore, codice having max(codice))
 order by squadra;