//! Streaming crypto (`encryptor` / `decryptor` / `crypto-output`) of the
//! wasi-http provider against the mock sidecar.

use std::time::Duration;

use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::crypto::{
    CryptoError, EncryptError, EncryptOptions,
};
use dapr_wasm_components_e2e::load_provider;
use dapr_wasm_components_e2e::mock::{MockSidecar, MOCK_CRYPTO_COMPONENT};

const WRITE_CHUNK: usize = 64 * 1024;

fn options() -> EncryptOptions {
    EncryptOptions {
        key_name: "mykey".to_string(),
        key_wrap_algorithm: "A256KW".to_string(),
        data_encryption_cipher: None,
        omit_decryption_key_name: None,
        decryption_key_name: None,
    }
}

/// Deterministic pseudo-random test payload (xorshift).
fn payload(len: usize) -> Vec<u8> {
    let mut state: u32 = 0x1234_5678;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect()
}

/// Encrypt 4 MiB through the streaming `encryptor` (64 KiB writes), read the
/// ciphertext back in odd-sized chunks, decrypt it through the streaming
/// `decryptor`, and compare. Both uploads must be streamed (chunked transfer
/// encoding), and the one-shot `decrypt` must accept the streamed ciphertext.
#[tokio::test]
async fn streaming_roundtrip() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    let crypto = provider.dapr_wasm_components_interfaces_crypto();
    let plaintext = payload(4 * 1024 * 1024);

    let encryptor = crypto
        .encryptor()
        .call_constructor(&mut store, MOCK_CRYPTO_COMPONENT, &options())
        .await
        .unwrap();
    for chunk in plaintext.chunks(WRITE_CHUNK) {
        crypto
            .encryptor()
            .call_write(&mut store, encryptor, chunk)
            .await
            .unwrap()
            .unwrap();
    }
    let output = crypto
        .encryptor()
        .call_finish(&mut store, encryptor)
        .await
        .unwrap()
        .unwrap();
    let mut ciphertext = Vec::new();
    let mut reads = 0;
    while let Some(chunk) = crypto
        .crypto_output()
        .call_read(&mut store, output, 100_000)
        .await
        .unwrap()
    {
        assert!(!chunk.is_empty() && chunk.len() <= 100_000);
        ciphertext.extend(chunk);
        reads += 1;
    }
    output.resource_drop_async(&mut store).await.unwrap();
    assert!(reads > 1, "output should be read in several chunks");
    assert_ne!(ciphertext, plaintext);

    let decryptor = crypto
        .decryptor()
        .call_constructor(&mut store, MOCK_CRYPTO_COMPONENT, None)
        .await
        .unwrap();
    for chunk in ciphertext.chunks(WRITE_CHUNK + 7) {
        crypto
            .decryptor()
            .call_write(&mut store, decryptor, chunk)
            .await
            .unwrap()
            .unwrap();
    }
    let output = crypto
        .decryptor()
        .call_finish(&mut store, decryptor)
        .await
        .unwrap()
        .unwrap();
    let mut decrypted = Vec::new();
    while let Some(chunk) = crypto
        .crypto_output()
        .call_read(&mut store, output, 1 << 20)
        .await
        .unwrap()
    {
        decrypted.extend(chunk);
    }
    output.resource_drop_async(&mut store).await.unwrap();
    assert!(decrypted == plaintext, "streaming roundtrip mismatch");

    let one_shot = crypto
        .call_decrypt(&mut store, MOCK_CRYPTO_COMPONENT, &ciphertext, None)
        .await
        .unwrap()
        .unwrap();
    assert!(one_shot == plaintext, "one-shot decrypt mismatch");

    let recorded = sidecar.recorded.lock().unwrap();
    let requests = &recorded.crypto_requests;
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].operation, "encrypt");
    assert_eq!(requests[0].body_len, plaintext.len());
    assert!(
        requests[0].chunked,
        "streaming encrypt must stream the body"
    );
    assert_eq!(requests[1].operation, "decrypt");
    assert_eq!(requests[1].body_len, ciphertext.len());
    assert!(
        requests[1].chunked,
        "streaming decrypt must stream the body"
    );
    assert_eq!(requests[2].body_len, ciphertext.len());
}

/// An unknown component is rejected before the sidecar reads the body. The
/// rejection must surface as `component-not-found` — at a `write` (the upload
/// broke) or at `finish` — never as a trap or a hang.
#[tokio::test]
async fn streaming_unknown_component() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    let crypto = provider.dapr_wasm_components_interfaces_crypto();
    let data = payload(8 * 1024 * 1024);

    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let encryptor = crypto
            .encryptor()
            .call_constructor(&mut store, "nope", &options())
            .await
            .unwrap();
        for chunk in data.chunks(WRITE_CHUNK) {
            let written = crypto
                .encryptor()
                .call_write(&mut store, encryptor, chunk)
                .await
                .unwrap();
            if let Err(e) = written {
                encryptor.resource_drop_async(&mut store).await.unwrap();
                return Err(e);
            }
        }
        crypto
            .encryptor()
            .call_finish(&mut store, encryptor)
            .await
            .unwrap()
            .map(|_| ())
    })
    .await
    .expect("streaming encrypt against an unknown component hung");

    assert!(
        matches!(
            result,
            Err(EncryptError::Crypto(CryptoError::ComponentNotFound(_)))
        ),
        "expected component-not-found, got {result:?}"
    );

    // The one-shot path maps the same daprd error the same way.
    let one_shot = crypto
        .call_encrypt(&mut store, "nope", b"hi", &options())
        .await
        .unwrap();
    assert!(
        matches!(
            one_shot,
            Err(EncryptError::Crypto(CryptoError::ComponentNotFound(_)))
        ),
        "expected component-not-found, got {one_shot:?}"
    );
}
