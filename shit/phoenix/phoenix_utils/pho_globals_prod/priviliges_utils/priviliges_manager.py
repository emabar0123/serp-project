import ctypes
import win32con
import win32api
import win32security
from ntsecuritycon import TokenPrivileges

PRIVILEGE_DISABLED = 0
PRIVILEGE_ENABLED = 2
PRIVILEGE_TYPE_POSITION = 0
TOKEN_VALUE_POSITION = 1


class PrivilegesManager:
    def __init__(self):
        if not ctypes.windll.shell32.IsUserAnAdmin():
            raise Exception("Must run in Administrator mode")

    @staticmethod
    def __get_privilege_win32_name(privilege_name: str) -> (None, str):
        """
        convert se privilege name to win32 se_privilege_name const
        :rtype: None, str
        :param privilege_name: str, formation SeBackupPrivilege
        :return: cont of win32security
        """
        privilege_name_formatted = f"SE_{privilege_name.upper()[2:-9]}_NAME"
        if hasattr(win32security, privilege_name_formatted):
            return getattr(win32security, privilege_name_formatted)
        if privilege_name in ([getattr(win32security, attr) for attr in dir(win32security) if attr.startswith("SE_")]):
            return privilege_name
        return None

    def set_privilege(self, privilege_name: str, enable: bool) -> bool:
        """
        set the privilege to the current process
        :param privilege_name: string represent the privilege name
        :param enable: true to enable privilege false to disable
        :return: tru if success false if fail, raises if bad privilege name
        """
        privilege_win32_type = self.__get_privilege_win32_name(privilege_name)
        if not privilege_win32_type:
            raise Exception("Unsupported privilege type")

        # get security token
        th = win32security.OpenProcessToken(win32api.GetCurrentProcess(),
                                            win32con.TOKEN_ADJUST_PRIVILEGES | win32con.TOKEN_QUERY)
        privileges = win32security.GetTokenInformation(th, TokenPrivileges)
        new_privileges = []
        new_flag = PRIVILEGE_ENABLED if enable else PRIVILEGE_DISABLED

        # prepare privileges tuple
        for privilege_tuple in privileges:
            if privilege_tuple[PRIVILEGE_TYPE_POSITION] == win32security.LookupPrivilegeValue(None, privilege_win32_type):
                new_privileges.append((privilege_tuple[PRIVILEGE_TYPE_POSITION], new_flag))
            else:
                new_privileges.append((privilege_tuple[PRIVILEGE_TYPE_POSITION], privilege_tuple[TOKEN_VALUE_POSITION]))

        # adjust privileges
        privileges = tuple(new_privileges)
        win32security.AdjustTokenPrivileges(th, False, privileges)
        return self.__check_privileges(th, privilege_win32_type, new_flag)

    @staticmethod
    def __check_privileges(token_handle: int, privilege_name: str, token_value: int) -> bool:
        """
        check if a privilege contain specific value
        :param token_handle: real type PyHANDLE
        :param privilege_name: string represent the privlage name
        :param token_value: the value to verify
        :return: true if the privilege is same as the token else false
        """
        privileges = win32security.GetTokenInformation(token_handle, TokenPrivileges)
        for privilege_tuple in privileges:
            if privilege_tuple[PRIVILEGE_TYPE_POSITION] != win32security.LookupPrivilegeValue(None, privilege_name):
                continue
            if privilege_tuple[TOKEN_VALUE_POSITION] == token_value:
                return True
        return False


def test():
    pm = PrivilegesManager()
    pm.set_privilege("SeBackupPrivilege", True)
    pm.set_privilege("SeDebugPrivilege", True)
    pm.set_privilege("SeRestorePrivilege", True)
    input()
    print("disabling")
    pm.set_privilege("SeBackupPrivilege", False)
    pm.set_privilege("SeDebugPrivilege", False)
    pm.set_privilege("SeRestorePrivilege", False)

    input()


if __name__ == '__main__':
    test()
