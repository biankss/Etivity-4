import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, ForeignKey, Table
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
from sqlalchemy.sql import func
from sqlalchemy.exc import IntegrityError

# --- 1. CONFIGURAZIONE E DEFINIZIONE MODELLI ORM ---

# 1.1 Definizione della Base
Base = declarative_base()

# Configurazione del Database (SQLite in memoria per test)
ENGINE = create_engine('sqlite:///:memory:', echo=False)
Session = sessionmaker(bind=ENGINE)

# 1.2 Tabella di Associazione N:M (Appartiene)
appartiene = Table('APPARTIENE', Base.metadata,
    Column('NomeUtente', String(50), ForeignKey('UTENTE.NomeUtente'), primary_key=True),
    Column('IDGruppo', Integer, ForeignKey('GRUPPO.IDGruppo'), primary_key=True)
)

# 1.3 Classi Modello

class Ruolo(Base):
    __tablename__ = 'RUOLO'
    IDRuolo = Column(Integer, primary_key=True)
    NomeRuolo = Column(String(50), nullable=False, unique=True)
    utenti = relationship("Utente", back_populates="ruolo")

class Gruppo(Base):
    __tablename__ = 'GRUPPO'
    IDGruppo = Column(Integer, primary_key=True)
    NomeGruppo = Column(String(50), nullable=False, unique=True)
    utenti = relationship("Utente", secondary=appartiene, back_populates="gruppi")

class ClienteFinale(Base):
    __tablename__ = 'CLIENTE_FINALE'
    NomeAzienda = Column(String(100), primary_key=True)
    Città = Column(String(50))
    NumTelefono = Column(String(20))
    KB = Column(Text)
    ticket_richiesti = relationship("Ticket", back_populates="cliente_richiedente")

class Utente(Base):
    __tablename__ = 'UTENTE'
    NomeUtente = Column(String(50), primary_key=True)
    IDRuolo = Column(Integer, ForeignKey('RUOLO.IDRuolo'), nullable=False)
    
    ruolo = relationship("Ruolo", back_populates="utenti")
    gruppi = relationship("Gruppo", secondary=appartiene, back_populates="utenti")
    commenti = relationship("Commento", back_populates="autore")
    ticket_gestiti = relationship("Ticket", back_populates="operatore_gestore")

class Ticket(Base):
    __tablename__ = 'TICKET'
    IDTicket = Column(Integer, primary_key=True)
    Titolo = Column(String(255), nullable=False)
    Descrizione = Column(Text)
    HApertura = Column(DateTime, nullable=False, default=func.now())
    HPresaInCarico = Column(DateTime)
    HChiusura = Column(DateTime)
    
    NomeAzienda = Column(String(100), ForeignKey('CLIENTE_FINALE.NomeAzienda'), nullable=False)
    NomeOperatoreGestore = Column(String(50), ForeignKey('UTENTE.NomeUtente'))
    
    cliente_richiedente = relationship("ClienteFinale", back_populates="ticket_richiesti")
    operatore_gestore = relationship("Utente", back_populates="ticket_gestiti")
    allegati = relationship("Allegato", back_populates="ticket")
    commenti = relationship("Commento", back_populates="ticket")

class Allegato(Base):
    __tablename__ = 'ALLEGATO'
    IDAllegato = Column(Integer, primary_key=True)
    DatiAllegato = Column(Text)
    IDTicket = Column(Integer, ForeignKey('TICKET.IDTicket'), nullable=False)
    
    ticket = relationship("Ticket", back_populates="allegati")

class Commento(Base):
    __tablename__ = 'COMMENTO'
    IDCommento = Column(Integer, primary_key=True)
    TestoCommento = Column(Text, nullable=False)
    IDTicket = Column(Integer, ForeignKey('TICKET.IDTicket'), nullable=False)
    NomeUtenteAutore = Column(String(50), ForeignKey('UTENTE.NomeUtente'), nullable=False)
    
    ticket = relationship("Ticket", back_populates="commenti")
    autore = relationship("Utente", back_populates="commenti")


# --- 2. FUNZIONI CRUD (CREATE, READ, UPDATE, DELETE) ---

def create_tables():
    """Operazione CREATE: Crea tutte le tabelle nel database."""
    Base.metadata.create_all(ENGINE)
    print("Database: Tabelle create con successo.")

def insert_initial_data(session):
    """Operazione CREATE: Inserisce i dati iniziali di test."""
    try:
        if session.query(Ruolo).count() > 0:
            print("Dati iniziali già presenti.")
            return

        # 1. Anagrafiche
        ruolo1 = Ruolo(IDRuolo=1, NomeRuolo='Tecnico L1')
        gruppo_net = Gruppo(IDGruppo=10, NomeGruppo='Network')
        cliente_acme = ClienteFinale(NomeAzienda='ACME S.p.A.', Città='Milano')
        
        session.add_all([ruolo1, gruppo_net, cliente_acme])
        session.commit()

        # 2. Utente e associazione N:M
        operatore = Utente(NomeUtente='mario_rossi', IDRuolo=1)
        session.add(operatore)
        session.commit()
        operatore.gruppi.append(gruppo_net) # Associa Mario al gruppo Network
        session.commit()

        # 3. Ticket
        ticket1 = Ticket(
            Titolo='Rete Lenta Ufficio 3',
            Descrizione='La connessione cade spesso.',
            NomeAzienda='ACME S.p.A.',
            NomeOperatoreGestore='mario_rossi'
        )
        session.add(ticket1)
        session.commit()
        
        # 4. Commento
        session.add(Commento(
            TestoCommento='Diagnosi iniziale completata.',
            IDTicket=ticket1.IDTicket,
            NomeUtenteAutore='mario_rossi'
        ))
        session.commit()
        print("Dati iniziali inseriti per testing CRUD.")

    except IntegrityError as e:
        session.rollback()
        print(f"Errore di integrità: {e}. Dati già presenti o vincolo violato.")
    except Exception as e:
        session.rollback()
        print(f"Errore durante l'inserimento dati: {e}")

def read_tickets(session, is_open=True):
    """Operazione READ: Legge i ticket, aperti o chiusi."""
    status = "APERTI" if is_open else "CHIUSI"
    print(f"\n--- Ticket {status} (Read) ---")
    
    query = session.query(Ticket).join(ClienteFinale)
    
    if is_open:
        # Filtro per ticket aperti (HChiusura IS NULL)
        tickets = query.filter(Ticket.HChiusura.is_(None)).all()
    else:
        # Filtro per ticket chiusi (HChiusura IS NOT NULL)
        tickets = query.filter(Ticket.HChiusura.is_not(None)).all()
        
    if not tickets:
        print("Nessun ticket trovato con lo stato specificato.")
        return []

    for t in tickets:
        print(f"ID: {t.IDTicket} | Titolo: {t.Titolo[:30]} | Cliente: {t.cliente_richiedente.NomeAzienda} | Gestore: {t.NomeOperatoreGestore or 'NON ASSEGNATO'}")
        
    return tickets

def update_ticket_status(session, ticket_id, action='chiudi', operatore='mario_rossi'):
    """Operazione UPDATE: Aggiorna lo stato o il gestore di un ticket."""
    try:
        # CORREZIONE 1: Uso della sintassi moderna (SQLAlchemy 2.0 style)
        ticket = session.get(Ticket, ticket_id)
        
        if not ticket:
            print(f"Ticket ID {ticket_id} non trovato.")
            return

        if action == 'chiudi':
            if ticket.HChiusura is None:
                ticket.HChiusura = datetime.datetime.now()
                print(f"UPDATE: Ticket ID {ticket_id} chiuso con successo.")
            else:
                print(f"Ticket ID {ticket_id} era già chiuso.")
        
        elif action == 'assegna' and ticket.NomeOperatoreGestore is None:
            ticket.NomeOperatoreGestore = operatore
            ticket.HPresaInCarico = datetime.datetime.now()
            print(f"UPDATE: Ticket ID {ticket_id} assegnato a {operatore} e preso in carico.")

        session.commit()
        
    except Exception as e:
        session.rollback()
        print(f"Errore durante l'aggiornamento: {e}")


def delete_ticket(session, ticket_id):
    """Operazione DELETE: Cancella un ticket e i suoi dettagli (commenti/allegati)."""
    try:
        # 1. Cancella dipendenze
        session.query(Allegato).filter(Allegato.IDTicket == ticket_id).delete(synchronize_session='fetch')
        session.query(Commento).filter(Commento.IDTicket == ticket_id).delete(synchronize_session='fetch')
        
        # 2. Cancella il Ticket
        # CORREZIONE 2: Uso della sintassi moderna (SQLAlchemy 2.0 style)
        ticket = session.get(Ticket, ticket_id)
        
        if ticket:
            session.delete(ticket)
            session.commit()
            print(f"\nDELETE: Ticket ID {ticket_id} e dettagli correlati cancellati con successo.")
        else:
            print(f"Ticket ID {ticket_id} non trovato per la cancellazione.")
            
    except Exception as e:
        session.rollback()
        print(f"Errore durante la cancellazione: {e}")


# --- 3. ESECUZIONE PRINCIPALE (DEMO CRUD) ---

if __name__ == '__main__':
    # Creazione delle tabelle (DDL)
    create_tables()
    session = Session()
    
    # 1. CREATE: Inserimento Dati Iniziali
    insert_initial_data(session)
    
    # --- DEMO CRUD ---
    
    # CREATE: Crea un nuovo ticket aperto
    nuovo_ticket = Ticket(Titolo='Errore in produzione urgente', Descrizione='Il sistema è bloccato.', NomeAzienda='ACME S.p.A.', HApertura=datetime.datetime.now())
    session.add(nuovo_ticket)
    session.commit()
    nuovo_ticket_id = nuovo_ticket.IDTicket
    print(f"\nCREATE: Creato nuovo ticket ID {nuovo_ticket_id}.")
    
    # READ: Leggi tutti i ticket aperti
    read_tickets(session) # CORREZIONE del NameError: usa la funzione corretta read_tickets
    
    # UPDATE: Assegna il nuovo ticket a mario_rossi
    update_ticket_status(session, nuovo_ticket_id, action='assegna', operatore='mario_rossi')
    
    # UPDATE: Chiudi il ticket iniziale (ID 1)
    update_ticket_status(session, 1, action='chiudi')
    
    # READ: Rivedi i ticket aperti e chiusi
    read_tickets(session, is_open=True)  # Controlla gli aperti
    read_tickets(session, is_open=False) # Controlla i chiusi
    
    # DELETE: Cancella il ticket appena creato (e i suoi dettagli)
    delete_ticket(session, nuovo_ticket_id)
    
    session.close()
