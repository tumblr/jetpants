{ buildPerlPackage, perlPackages, fetchurl, tree, perl }:
buildPerlPackage {
  name = "percona-toolkit-2.2.20-1";

  src = fetchurl {
    url = "https://www.percona.com/downloads/percona-toolkit/2.2.20/deb/percona-toolkit_2.2.20-1.tar.gz";
    sha256 = "1axlavcnz0qlzx9lcjjkvzl9vh7qk6z63ab72vf08chls2mzafn8";
  };

  propagatedBuildInputs = [
    perlPackages.DBI
    perlPackages.DBDmysql
  ];

  outputs = [ "out" ];
}
