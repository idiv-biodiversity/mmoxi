//! Information about users.

use std::process::Command;

use bstr::ByteSlice;

/// Optionally returns the username of the user by their UID.
///
/// This function uses `getent passwd`. It returns `None` if either this
/// command fails, the user does not exist or the username is not UTF-8.
#[must_use]
pub fn by_uid(uid: impl AsRef<str>) -> Option<String> {
    let uid = uid.as_ref();

    let mut cmd = Command::new("getent");
    cmd.arg("passwd");
    cmd.arg(uid);

    cmd.output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| {
            output
                .stdout
                .splitn_str(2, ":")
                .next()
                .and_then(|username| {
                    username.to_str().ok().map(ToOwned::to_owned)
                })
        })
}
