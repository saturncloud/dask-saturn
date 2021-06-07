"""
Classes and functions for configuring the connection to a Saturn Dask cluster
from outside of the Saturn installation.
"""

import requests

from cryptography import x509
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from distributed.security import Security
from typing import List, Tuple
from urllib.parse import urljoin

from .settings import Settings


class ExternalConnection:
    """
    DEPRECATED: Stores settings for connecting to Saturn from an external location, and manages
    TLS certificates for communicating with the Dask Scheduler.
    """

    def __init__(self, *args, **kwargs):
        raise RuntimeError(
            "ExternalConnection is no longer supported. "
            "Instead, set the env vars: ``SATURN_TOKEN`` and ``SATURN_BASE_URL`` "
            "as indicated in the Saturn Cloud UI. If those env vars are set, an external "
            "connection will be automatically set up."
        )


def _client_tls(
    settings: Settings, dask_cluster_id: str
) -> (rsa.RSAPrivateKey, x509.Certificate, x509.Certificate):
    """
    Generate an RSA key and certificate signing request. Send the CSR to Saturn to be signed
    by the Dask cluster CA, and return the key, cert, and CA.
    """
    csr, key = _create_csr(f"Dask Client {dask_cluster_id}")

    url = urljoin(settings.url, f"/api/dask_clusters/{dask_cluster_id}/csr")
    resp = requests.post(
        url,
        data=_serialize_csr(csr),
        headers=settings.headers,
    )
    if not resp.ok:
        resp.raise_for_status()

    cert_chain = _deserialize_cert_chain(resp.content)
    if len(cert_chain) <= 1:
        raise ValueError("Invalid certificate chain returned from server")
    cert = cert_chain[0]
    ca_cert = cert_chain[1]

    return key, cert, ca_cert


def _security(settings: Settings, dask_cluster_id: str) -> Security:
    """
    Return Dask distributed security for connecting to the given dask cluster's scheduler
    over a public endpoint.
    """
    key, cert, ca_cert = _client_tls(settings, dask_cluster_id)
    key_contents = _serialize_key(key).decode()
    cert_contents = _serialize_cert(cert).decode()
    ca_cert_contents = _serialize_cert(ca_cert).decode()
    return Security(
        tls_client_key=key_contents,
        tls_client_cert=cert_contents,
        tls_ca_file=ca_cert_contents,
        require_encryption=True,
    )


def _create_csr(
    common_name: str,
) -> Tuple[x509.CertificateSigningRequest, rsa.RSAPrivateKeyWithSerialization]:
    """Return RSA key and certificate signing request"""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())
    csr = (
        x509.CertificateSigningRequestBuilder()
        .subject_name(
            x509.Name(
                [
                    x509.NameAttribute(NameOID.COMMON_NAME, common_name),
                ]
            )
        )
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .sign(key, hashes.SHA256(), default_backend())
    )
    return csr, key


def _serialize_csr(csr: x509.CertificateSigningRequest) -> bytes:
    """Return the PEM bytes from a certificate signing request"""
    return csr.public_bytes(serialization.Encoding.PEM)


def _serialize_cert(cert: x509.Certificate) -> bytes:
    """Return the PEM bytes from an X509 certificate"""
    return cert.public_bytes(serialization.Encoding.PEM)


def _serialize_key(key: rsa.RSAPrivateKeyWithSerialization) -> bytes:
    """Return the PEM bytes from an RSA private key"""
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _deserialize_cert_chain(data: bytes) -> List[x509.Certificate]:
    """Return the X509 certificate chain from the given PEM bytes"""
    pem_start_line = b"-----BEGIN CERTIFICATE-----\n"
    chain = data.split(pem_start_line)
    return [
        x509.load_pem_x509_certificate(pem_start_line + cert_bytes, default_backend())
        for cert_bytes in chain
        if cert_bytes
    ]
