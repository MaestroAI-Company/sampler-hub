//icon and company shown in the windows file properties
fn main() {
    //check target os for cross builds
    #[cfg(windows)]
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        let mut resource = tauri_winres::WindowsResource::new();
        resource
            .set_icon("assets/icons/icon.ico")
            .set("ProductName", "Sampler-Hub")
            .set("FileDescription", "Sampler-Hub")
            .set("CompanyName", "Maestro AI")
            .set("LegalCopyright", "© Maestro AI");
        resource.compile().expect("windows resources");
    }
}
