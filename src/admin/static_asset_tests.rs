use super::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[cfg(unix)]
#[test]
fn assets_and_spa_fallback_refuse_symlinks_outside_the_configured_root() -> TestResult {
    use std::os::unix::fs::symlink;
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("studio");
    fs::create_dir(&root)?;
    fs::write(root.join("index.html"), b"public")?;
    let secret = directory.path().join("secret.txt");
    fs::write(&secret, b"must remain private")?;
    symlink(&secret, root.join("exposed.txt"))?;
    assert_eq!(
        studio_static_response(&root, "/exposed.txt", &Method::GET).status(),
        StatusCode::NOT_FOUND
    );
    fs::remove_file(root.join("index.html"))?;
    symlink(&secret, root.join("index.html"))?;
    assert_eq!(
        studio_static_response(&root, "/spa-route", &Method::GET).status(),
        StatusCode::NOT_FOUND
    );
    Ok(())
}

#[tokio::test]
async fn head_reports_get_length_and_oversized_assets_are_refused() -> TestResult {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("index.html"), b"public")?;
    let response = studio_static_response(directory.path(), "/", &Method::HEAD);
    assert_eq!(response.headers()[CONTENT_LENGTH], "6");
    assert!(
        axum::body::to_bytes(response.into_body(), 1024)
            .await?
            .is_empty()
    );
    let file = fs::File::create(directory.path().join("huge.bin"))?;
    file.set_len(MAX_STUDIO_ASSET_BYTES + 1)?;
    assert_eq!(
        studio_static_response(directory.path(), "/huge.bin", &Method::GET).status(),
        StatusCode::NOT_FOUND
    );
    Ok(())
}
