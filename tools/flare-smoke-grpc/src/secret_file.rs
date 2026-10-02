//! 读取私有秘密文件与口令策略。错误信息只提文件用途与路径，从不带内容。

use std::fs;
use std::io::Read;
use std::path::Path;

use crate::error::{Result, ensure, err, fail};

pub const MAX_KEY_FILE: u64 = 16 * 1024;
pub const MAX_PASSWORD_FILE: u64 = 4 * 1024;
pub const MIN_DB_PASSWORD_CHARS: usize = 16;

/// 仓库里出现过的默认/示例口令与常见弱口令；比较时忽略大小写。
/// 长度下限已经挡住其中大多数，这里显式列出是为了让拒绝原因可读。
pub const DEFAULT_DB_PASSWORDS: [&str; 16] = [
    "flare123",
    "flare",
    "flare2",
    "flare_core",
    "flare_password",
    "postgres",
    "password",
    "passw0rd",
    "changeme",
    "change-me",
    "secret",
    "admin",
    "root",
    "fixture",
    "123456",
    "12345678",
];

/// 必须是本人可读、组和其他人无任何权限（600/400）的普通文件；不跟随符号链接。
pub fn read_private_file(path: &Path, what: &str, max_len: u64) -> Result<String> {
    let shown = path.display();
    let info = fs::symlink_metadata(path).map_err(|_| err!("{what} file {shown} is missing"))?;
    ensure!(
        info.file_type().is_file(),
        "{what} file {shown} must be a regular file, not a symlink or directory"
    );
    check_private_mode(&info, what, &shown.to_string())?;
    ensure!(
        info.len() <= max_len,
        "{what} file {shown} is larger than {max_len} bytes"
    );
    let mut text = String::new();
    fs::File::open(path)
        .and_then(|file| file.take(max_len + 1).read_to_string(&mut text))
        .map_err(|_| err!("{what} file {shown} is not readable UTF-8"))?;
    ensure!(
        text.len() as u64 <= max_len,
        "{what} file {shown} is larger than {max_len} bytes"
    );
    Ok(text)
}

#[cfg(unix)]
fn check_private_mode(info: &fs::Metadata, what: &str, shown: &str) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = info.permissions().mode() & 0o777;
    ensure!(
        mode & 0o077 == 0,
        "{what} file {shown} must not be accessible by group or others (chmod 600)"
    );
    Ok(())
}

#[cfg(not(unix))]
fn check_private_mode(_: &fs::Metadata, what: &str, _: &str) -> Result<()> {
    fail!("{what}: private file permissions can only be verified on Unix")
}

/// 读取数据库口令：单行、可打印、不是默认/弱口令。
pub fn read_db_password(path: &Path, user: &str, database: &str) -> Result<String> {
    let text = read_private_file(path, "database password", MAX_PASSWORD_FILE)?;
    let password = text.strip_suffix('\n').unwrap_or(&text);
    let password = password.strip_suffix('\r').unwrap_or(password);
    ensure!(
        !password.contains(['\n', '\r']),
        "database password file must hold exactly one line"
    );
    check_db_password(password, user, database)?;
    Ok(password.to_owned())
}

pub fn check_db_password(password: &str, user: &str, database: &str) -> Result<()> {
    ensure!(
        !password.is_empty() && !password.chars().any(char::is_control),
        "database password must be one line of printable characters"
    );
    let lowered = password.to_ascii_lowercase();
    if DEFAULT_DB_PASSWORDS.contains(&lowered.as_str()) {
        fail!("database password is a known default; use the installation's generated password");
    }
    ensure!(
        password.chars().count() >= MIN_DB_PASSWORD_CHARS,
        "database password is shorter than {MIN_DB_PASSWORD_CHARS} characters; default or weak passwords are refused"
    );
    for name in [user, database] {
        let name = name.to_ascii_lowercase();
        ensure!(
            name.len() < 3 || !lowered.contains(&name),
            "database password contains the role or database name; default or weak passwords are refused"
        );
    }
    let mut chars = password.chars();
    let first = chars.next();
    ensure!(
        chars.any(|char| Some(char) != first),
        "database password repeats one character; default or weak passwords are refused"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    const STRONG: &str = "Qx7mV2pL9sKd4RtN8wYb3HjF";

    fn temp_dir() -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("flare-smoke-grpc-test-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&dir).unwrap();
        dir
    }

    fn write(dir: &Path, name: &str, text: &str, mode: u32) -> std::path::PathBuf {
        let path = dir.join(name);
        fs::write(&path, text).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    #[test]
    fn private_files_are_required() {
        let dir = temp_dir();
        let good = write(&dir, "good", STRONG, 0o600);
        assert_eq!(read_private_file(&good, "key", 64).unwrap(), STRONG);
        let read_only = write(&dir, "ro", STRONG, 0o400);
        assert!(read_private_file(&read_only, "key", 64).is_ok());

        for mode in [0o640, 0o604, 0o644, 0o660, 0o610] {
            let path = write(&dir, &format!("open-{mode:o}"), STRONG, mode);
            let error = read_private_file(&path, "key", 64).unwrap_err().to_string();
            assert!(error.contains("chmod 600"), "{mode:o}: {error}");
            assert!(!error.contains(STRONG));
        }

        let link = dir.join("link");
        std::os::unix::fs::symlink(&good, &link).unwrap();
        assert!(
            read_private_file(&link, "key", 64)
                .unwrap_err()
                .to_string()
                .contains("regular file")
        );
        assert!(
            read_private_file(&dir.join("absent"), "key", 64)
                .unwrap_err()
                .to_string()
                .contains("missing")
        );
        assert!(
            read_private_file(&good, "key", 8)
                .unwrap_err()
                .to_string()
                .contains("larger")
        );
        assert!(read_private_file(&dir, "key", 64).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn default_and_weak_passwords_are_refused() {
        for password in [
            "flare123",
            "FLARE123",
            "postgres",
            "password",
            "fixture",
            "short-random1",
            "aaaaaaaaaaaaaaaaaaaa",
            "flare_core_app-0123456789",
            "x-flare_core-0123456789",
            "",
            "tab\tinside-0123456789",
        ] {
            assert!(
                check_db_password(password, "flare_core_app", "flare_core").is_err(),
                "{password:?}"
            );
        }
        check_db_password(STRONG, "flare_core_app", "flare_core").unwrap();
    }

    #[test]
    fn password_file_holds_one_line() {
        let dir = temp_dir();
        let path = write(&dir, "pw", &format!("{STRONG}\n"), 0o600);
        assert_eq!(
            read_db_password(&path, "flare_core_app", "flare_core").unwrap(),
            STRONG
        );
        let crlf = write(&dir, "crlf", &format!("{STRONG}\r\n"), 0o600);
        assert_eq!(
            read_db_password(&crlf, "flare_core_app", "flare_core").unwrap(),
            STRONG
        );
        let two = write(&dir, "two", &format!("{STRONG}\n{STRONG}\n"), 0o600);
        assert!(read_db_password(&two, "flare_core_app", "flare_core").is_err());
        let default = write(&dir, "default", "flare123\n", 0o600);
        let error = read_db_password(&default, "flare_core_app", "flare_core")
            .unwrap_err()
            .to_string();
        assert!(error.contains("known default"), "{error}");
        assert!(!error.contains("flare123"), "{error}");
        let open = write(&dir, "open", STRONG, 0o644);
        assert!(read_db_password(&open, "flare_core_app", "flare_core").is_err());
        fs::remove_dir_all(dir).unwrap();
    }
}
