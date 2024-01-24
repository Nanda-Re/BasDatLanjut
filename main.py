import uuid
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.query import dict_factory

# Buat koneksi ke cluster
cluster = Cluster(['127.0.0.1'],port=2020)
session = cluster.connect('toko_baju')

session.row_factory = dict_factory

# CRUD operations for 'item_transaksi' table
def create_item_transaksi(transaksi_id, produk_id, harga, jumlah):
    query = """
    INSERT INTO item_transaksi (transaksi_id, produk_id, harga, jumlah)
    VALUES (%s, %s, %s, %s)
    """
    session.execute(query, (transaksi_id, produk_id, harga, jumlah))

def read_item_transaksi():
    query = "SELECT * FROM item_transaksi"
    result = session.execute(query)
    return result.all()

def update_item_transaksi(transaksi_id, produk_id, new_harga, new_jumlah):
    query = "UPDATE item_transaksi SET harga = %s, jumlah = %s WHERE transaksi_id = %s AND produk_id = %s"
    session.execute(query, (new_harga, new_jumlah, transaksi_id, produk_id))

def delete_item_transaksi(transaksi_id, produk_id):
    query = "DELETE FROM item_transaksi WHERE transaksi_id = %s AND produk_id = %s"
    session.execute(query, (transaksi_id, produk_id))

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

def delete_produk_baju(id):
    query = "DELETE FROM produk_baju WHERE id = %s"
    session.execute(query, (id,))

# CRUD operations for 'transaksi' table
def create_transaksi(id, pelanggan_id, total_harga, waktu):
    query = """
    INSERT INTO transaksi (id, pelanggan_id, total_harga, waktu)
    VALUES (%s, %s, %s, %s)
    """
    session.execute(query, (id, pelanggan_id, total_harga, waktu))

def read_transaksi():
    query = "SELECT * FROM transaksi"
    result = session.execute(query)
    return result.all()

def update_transaksi(id, new_pelanggan_id, new_total_harga, new_waktu):
    query = "UPDATE transaksi SET pelanggan_id = %s, total_harga = %s, waktu = %s WHERE id = %s"
    session.execute(query, (new_pelanggan_id, new_total_harga, new_waktu, id))

def delete_transaksi(id):
    query = "DELETE FROM transaksi WHERE id = %s"
    session.execute(query, (id,))

# Example usage for 'item_transaksi'
transaksi_id = uuid.uuid4()
produk_id = uuid.uuid4()
create_item_transaksi(transaksi_id, produk_id, 50.0, 2)
print("Data item_transaksi:")
print(read_item_transaksi())

update_item_transaksi(transaksi_id, produk_id, 60.0, 3)
print("Data item_transaksi after update:")
print(read_item_transaksi())

# delete_item_transaksi(transaksi_id, produk_id)
# print("Data item_transaksi after delete:")
# print(read_item_transaksi())

# Example usage for 'pelanggan'
pelanggan_id = uuid.uuid4()
create_pelanggan(pelanggan_id, "Jl. Pahlawan No. 123", "john.doe@example.com", "John Doe", "123456789")
print("Data pelanggan:")
print(read_pelanggan())

update_pelanggan(pelanggan_id, "Jl. Merdeka No. 456", "john.updated@example.com", "John Updated", "987654321")
print("Data pelanggan after update:")
print(read_pelanggan())

# delete_pelanggan(pelanggan_id)
# print("Data pelanggan after delete:")
# print(read_pelanggan())

# Example usage for 'produk_baju'
produk_id = uuid.uuid4()
create_produk_baju(produk_id, 49.99, "Kemeja", "Kemeja Putih", 100, "XL")
print("Data produk_baju:")
print(read_produk_baju())

update_produk_baju(produk_id, 54.99, "Kemeja", "Kemeja Merah", 80, "L")
print("Data produk_baju after update:")
print(read_produk_baju())

# delete_produk_baju(produk_id)
# print("Data produk_baju after delete:")
# print(read_produk_baju())

# Example usage for 'transaksi'
transaksi_id = uuid.uuid4()
create_transaksi(transaksi_id, pelanggan_id, 150.0, datetime.now())
print("Data transaksi:")
print(read_transaksi())

update_transaksi(transaksi_id, pelanggan_id, 200.0, datetime.now())
print("Data transaksi after update:")
print(read_transaksi())

# delete_transaksi(transaksi_id)
# print("Data transaksi after delete:")
# print(read_transaksi())

# Close connection
cluster.shutdown()

