import uuid
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.query import dict_factory

# Buat koneksi ke cluster
cluster = Cluster(['127.0.0.1'], port=2020)
session = cluster.connect('toko_baju')

session.row_factory = dict_factory

# CRUD operations for 'item_transaksi' table
def create_item_transaksi(item_transaksi_id, produk_id, harga, jumlah):
    query = """
    INSERT INTO item_transaksi (transaksi_id, produk_id, harga, jumlah)
    VALUES (%s, %s, %s, %s)
    """
    session.execute(query, (item_transaksi_id, produk_id, harga, jumlah))

def read_item_transaksi():
    query = "SELECT * FROM item_transaksi"
    result = session.execute(query)
    return result.all()

def update_item_transaksi(item_transaksi_id, produk_id, new_harga, new_jumlah):
    query = "UPDATE item_transaksi SET harga = %s, jumlah = %s WHERE transaksi_id = %s AND produk_id = %s"
    session.execute(query, (new_harga, new_jumlah, item_transaksi_id, produk_id))

def delete_item_transaksi(item_transaksi_id, produk_id):
    query = "DELETE FROM item_transaksi WHERE transaksi_id = %s AND produk_id = %s"
    session.execute(query, (item_transaksi_id, produk_id))

# CRUD operations for 'pelanggan' table
def create_pelanggan(id, alamat, email, nama, telepon):
    query = """
    INSERT INTO pelanggan (id, alamat, email, nama, telepon)
    VALUES (%s, %s, %s, %s, %s)
    """
    session.execute(query, (id, alamat, email, nama, telepon))

def read_pelanggan():
    query = "SELECT * FROM pelanggan"
    result = session.execute(query)
    return result.all()

def update_pelanggan(id, new_alamat, new_email, new_nama, new_telepon):
    query = "UPDATE pelanggan SET alamat = %s, email = %s, nama = %s, telepon = %s WHERE id = %s"
    session.execute(query, (new_alamat, new_email, new_nama, new_telepon, id))

def delete_pelanggan(id):
    query = "DELETE FROM pelanggan WHERE id = %s"
    session.execute(query, (id,))




# CRUD operations for 'produk_baju' table
def create_produk_baju(id, harga, jenis_baju, nama_produk, stok, ukuran):
    query = """
    INSERT INTO produk_baju (id, harga, jenis_baju, nama_produk, stok, ukuran)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (id, harga, jenis_baju, nama_produk, stok, ukuran))

def read_produk_baju():
    query = "SELECT * FROM produk_baju"
    result = session.execute(query)
    return result.all()

def update_produk_baju(id, new_harga, new_jenis_baju, new_nama_produk, new_stok, new_ukuran):
    query = "UPDATE produk_baju SET harga = %s, jenis_baju = %s, nama_produk = %s, stok = %s, ukuran = %s WHERE id = %s"
    session.execute(query, (new_harga, new_jenis_baju, new_nama_produk, new_stok, new_ukuran, id))

def delete_produk_baju(produk_id):
    query = "DELETE FROM produk_baju WHERE id = %s"
    session.execute(query, (produk_id,))

# CRUD operations for 'transaksi' table
def create_transaksi(transaksi_id, pelanggan_id, total_harga, waktu):
    query = """
    INSERT INTO transaksi (id, pelanggan_id, total_harga, waktu)
    VALUES (%s, %s, %s, %s)
    """
    session.execute(query, (transaksi_id, pelanggan_id, total_harga, waktu))

def read_transaksi():
    query = "SELECT * FROM transaksi "
    result = session.execute(query)
    return result.all()

def update_transaksi(transaksi_id, new_pelanggan_id, new_total_harga, new_waktu):
    query = "UPDATE transaksi SET pelanggan_id = %s, total_harga = %s, waktu = %s WHERE id = %s"
    session.execute(query, (new_pelanggan_id, new_total_harga, new_waktu, transaksi_id))

def delete_transaksi(transaksi_id):
    query = "DELETE FROM transaksi WHERE id = %s"
    session.execute(query, (transaksi_id,))

# Dynamic CRUD operation execution
def execute_dynamic_crud():
    while True:
        print("\n*** Dynamic CRUD Operation Menu ***")
        print("1. Create")
        print("2. Read")
        print("3. Update")
        print("4. Delete")
        print("0. Exit")

        choice = input("Enter your choice (0-4): ").strip()

        if choice == '0':
            break
        elif choice in ['1', '2', '3', '4']:
            table_name = input("Enter table name (item_transaksi/pelanggan/produk_baju/transaksi): ").strip()
            operation = ['create', 'read', 'update', 'delete'][int(choice) - 1]

            if table_name == 'item_transaksi' and operation in ['create', 'read', 'update', 'delete']:
                item_transaksi_id =int(input("Enter transaksi id: "))
                produk_id = int(input("Enter produk_id: "))

                if operation == 'create':
                    harga = float(input("Enter harga: "))
                    jumlah = int(input("Enter jumlah: "))
                    create_item_transaksi(item_transaksi_id, produk_id, harga, jumlah)
                elif operation == 'read':
                    print(read_item_transaksi())
                elif operation == 'update':
                    new_harga = float(input("Enter new harga: "))
                    new_jumlah = int(input("Enter new jumlah: "))
                    update_item_transaksi(item_transaksi_id, produk_id, new_harga, new_jumlah)
                elif operation == 'delete':
                    delete_item_transaksi(item_transaksi_id, produk_id)

            elif table_name == 'pelanggan' and operation in ['create', 'read', 'update', 'delete']:

                if operation == 'create':
                    pelanggan_id = int(input("Enter pelanggan id: "))
                    alamat = input("Enter alamat: ")
                    email = input("Enter email: ")
                    nama = input("Enter nama: ")
                    telepon = input("Enter telepon: ")
                    create_pelanggan(pelanggan_id, alamat, email, nama, telepon)
                elif operation == 'read':
                    print(read_pelanggan())
                elif operation == 'update':
                    pelanggan_id = int(input("Enter pelanggan id: "))
                    new_alamat = input("Enter new alamat: ")
                    new_email = input("Enter new email: ")
                    new_nama = input("Enter new nama: ")
                    new_telepon = input("Enter new telepon: ")
                    update_pelanggan(pelanggan_id, new_alamat, new_email, new_nama, new_telepon)
                elif operation == 'delete':
                    pelanggan_id = int(input("Enter pelanggan id: "))
                    delete_pelanggan(pelanggan_id)

            elif table_name == 'produk_baju' and operation in ['create', 'read', 'update', 'delete']:


                if operation == 'create':
                    produk_id = int(input("Enter produk_id: "))
                    harga = float(input("Enter harga: "))
                    jenis_baju = input("Enter jenis baju: ")
                    nama_produk = input("Enter nama produk: ")
                    stok = int(input("Enter stok: "))
                    ukuran = input("Enter ukuran: ")
                    create_produk_baju(produk_id, harga, jenis_baju, nama_produk, stok, ukuran)
                elif operation == 'read':
                    print(read_produk_baju())
                elif operation == 'update':
                    produk_id = int(input("Enter produk_id: "))
                    new_harga = float(input("Enter new harga: "))
                    new_jenis_baju = input("Enter new jenis baju: ")
                    new_nama_produk = input("Enter new nama produk: ")
                    new_stok = int(input("Enter new stok: "))
                    new_ukuran = input("Enter new ukuran: ")
                    update_produk_baju(produk_id, new_harga, new_jenis_baju, new_nama_produk, new_stok, new_ukuran)
                elif operation == 'delete':
                    produk_id = int(input("Enter produk_id: "))
                    delete_produk_baju(produk_id)

            elif table_name == 'transaksi' and operation in ['create', 'read', 'update', 'delete']:


                if operation == 'create':
                    transaksi_id = int(input("Enter transaksi_id: "))
                    pelanggan_id = int(input("Enter pelanggan_id: "))
                    total_harga = float(input("Enter total harga: "))
                    waktu = datetime.now()
                    create_transaksi(transaksi_id, pelanggan_id, total_harga, waktu)
                elif operation == 'read':
                    print(read_transaksi())
                elif operation == 'update':
                    transaksi_id = int(input("Enter transaksi_id: "))
                    pelanggan_id = int(input("Enter pelanggan_id: "))
                    new_pelanggan_id = int(input("Enter new_pelanggan_id: "))
                    new_total_harga = float(input("Enter new total harga: "))
                    new_waktu = datetime.now()
                    update_transaksi(transaksi_id, pelanggan_id, new_pelanggan_id, new_total_harga, new_waktu)
                elif operation == 'delete':
                    transaksi_id = int(input("Enter transaksi_id: "))
                    delete_transaksi(transaksi_id)

            else:
                print("Invalid table name or operation")
        else:
            print("Invalid choice. Please enter a number between 0 and 4.")

# Execute dynamic CRUD operations using a do-while loop
execute_dynamic_crud()

# Close connection
cluster.shutdown()
