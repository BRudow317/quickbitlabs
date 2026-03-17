import base64
import struct
from cryptography.fernet import Fernet


def stream_and_tag(input_path, output_path):
    """
    Streams a file, replacing CR with <<CR>> and LF with <<LF>>.
    """
    with open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
        for line in f_in:
            processed = line.replace(b"\r", b"<<CR>>").replace(b"\n", b"<<LF>>")
            f_out.write(processed)


def b64_encode(data: bytes) -> str:
    """Encodes bytes to a Base64 string."""
    return base64.b64encode(data).decode("utf-8")


def b64_decode(data: str) -> bytes:
    """Decodes a Base64 string back to bytes."""
    return base64.b64decode(data)


def generate_and_save_key(key_path="secret.key"):
    """Generates a secure key and saves it to a file."""
    key = Fernet.generate_key()
    with open(key_path, "wb") as key_file:
        key_file.write(key)
    return key


def encrypt_data(data: bytes, key: bytes) -> bytes:
    """Encrypts bytes using the provided Fernet key."""
    f = Fernet(key)
    return f.encrypt(data)


def decrypt_data(token: bytes, key: bytes) -> bytes:
    """Decrypts a Fernet token back to the original bytes."""
    f = Fernet(key)
    return f.decrypt(token)


def stream_encrypt_file(input_path, output_path, key, chunk_size=64 * 1024):
    """Streams a file and encrypts it in chunks."""
    f = Fernet(key)

    with open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
        while True:
            chunk = f_in.read(chunk_size)
            if not chunk:
                break

            encrypted_chunk = f.encrypt(chunk)
            # Store chunk size first (unsigned 4-byte int), then encrypted bytes
            f_out.write(struct.pack("<I", len(encrypted_chunk)))
            f_out.write(encrypted_chunk)


def stream_decrypt_file(input_path, output_path, key):
    """Decrypts a file that was encrypted in chunks."""
    f = Fernet(key)

    with open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
        while True:
            size_data = f_in.read(4)
            if not size_data:
                break

            chunk_size = struct.unpack("<I", size_data)[0]
            encrypted_chunk = f_in.read(chunk_size)
            f_out.write(f.decrypt(encrypted_chunk))