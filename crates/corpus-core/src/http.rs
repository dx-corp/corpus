//! HTTP policy shared by Corpus agents, operators and server adapters.

use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Artifact uploads, registry layers and sample downloads can be hundreds of
/// MiB (the agent defaults to a 256 MiB artifact limit). Give those requests
/// five minutes including their bodies; metadata and polling keep the shorter
/// client deadline.
pub const ARTIFACT_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

/// Keep credentials, sample bytes and signed events at the configured URL.
/// OCI blob reads opt into their separate CDN redirect policy.
pub fn client_builder() -> reqwest::ClientBuilder {
    builder_with_timeouts(CONNECT_TIMEOUT, REQUEST_TIMEOUT)
}

fn builder_with_timeouts(connect: Duration, request: Duration) -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .connect_timeout(connect)
        .timeout(request)
        .redirect(reqwest::redirect::Policy::none())
}

/// Construct a client using the static Corpus transport policy.
pub fn client() -> reqwest::Client {
    client_builder()
        .build()
        .expect("static Corpus HTTP client configuration must be valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn short_deadline_client() -> reqwest::Client {
        builder_with_timeouts(Duration::from_secs(1), Duration::from_millis(100))
            .no_proxy()
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn stalled_response_headers_time_out() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let count = stream.read(&mut request).await.unwrap();
            assert_ne!(count, 0, "fixture must receive request bytes");
            std::future::pending::<()>().await;
        });
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            short_deadline_client()
                .get(format!("http://{address}/"))
                .send(),
        )
        .await
        .expect("client must enforce its request deadline")
        .unwrap_err();
        server.abort();
        assert!(error.is_timeout(), "{error}");
    }

    #[tokio::test]
    async fn stalled_response_body_times_out() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let count = stream.read(&mut request).await.unwrap();
            assert_ne!(count, 0, "fixture must receive request bytes");
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\na")
                .await
                .unwrap();
            std::future::pending::<()>().await;
        });
        let response = short_deadline_client()
            .get(format!("http://{address}/"))
            .send()
            .await
            .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(2), response.bytes())
            .await
            .expect("the request deadline must also cover body reads")
            .unwrap_err();
        server.abort();
        assert!(error.is_timeout(), "{error}");
    }

    #[tokio::test]
    async fn authenticated_posts_do_not_follow_redirects() {
        let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let target = destination.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let count = stream.read(&mut request).await.unwrap();
            assert_ne!(count, 0, "fixture must receive request bytes");
            stream
                .write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{target}/other\r\nContent-Length: 0\r\n\r\n").as_bytes())
                .await
                .unwrap();
        });
        let response = client_builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("http://{address}/"))
            .bearer_auth("fixture-token")
            .body("fixture sample")
            .send()
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), destination.accept())
                .await
                .is_err()
        );
    }
}
