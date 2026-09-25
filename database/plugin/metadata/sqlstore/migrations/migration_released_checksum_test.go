// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migrations

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// releasedMigrationChecksums pins the checksum every shipped migration version
// produces on each dialect. The runner compares a database's recorded checksum
// against the registry's and refuses to start on a mismatch (ErrChecksumDrift),
// so editing the SQL of a version that already exists in the field bricks every
// node that ran it until a resync.
//
// A new migration adds its own row in the same change that adds the migration.
// An existing row is never edited: a statement that must change gets a new
// version instead. The checksum covers the dialect-translated statements, and
// translation reads the schema as a whole, so an unrelated later migration that
// perturbs an earlier version's rendering shows up here too.
var releasedMigrationChecksums = map[string]map[int]string{
	"sqlite": {
		1:  "05bf00417fe33753f0fae9400ee2f203e4df53f29e630a8c51eed2e0857a6d85",
		2:  "5412f9074b168bb800a9991c101cebd11741ffe50743736c173adcdca4bfb6ad",
		3:  "e7bc6798c8e758fb510ea1eff98bcdd65f66319f497953692e1bfb554712aa53",
		4:  "ea8c40a9078ea3b9af5d7a341c07e2a4169738f6b50739b02226dcadd7179b90",
		5:  "cf9ed62310cc26450d2e3b611b2deb076268c745204d420a8cf85bbbe1bfa8f1",
		6:  "16540b51745645a9afab75a9eb6fe0a50f032849138c9c72e96e73d3fa93a36e",
		7:  "540d8b058cc01fd69756512a866f0abe9d4b7a302c095b32abdd09653f7a32cc",
		8:  "be9867f6098ce919100c9f3af6b560b51609e48371edaf483f913d30a091a8b7",
		9:  "c95939ee7ee3f520d06588c2aef5199ca0ecf9f7bb023c2d3b5b913f19830fcd",
		10: "f557fb538e04bbc8281ceffec9d25e9277710b088557b30746622b7a6912674f",
		11: "ee1ccd79d0bae126e48b6118d5dea236279afb40f388d245f7cadaf991a86310",
		12: "22ee9eef21de063843e9a79d71f91a9ca553415c20e0b1d626b0fa9c6bfcd44d",
		13: "00a8db163a8e3d47467ed460db3b655e63ff68b744b8ecd5714b44ce59b1131d",
		14: "683ca8bbbc599d07a2714d6374db81183e66edab2358f60ca2a3bd170d1e5d6d",
		15: "7f398f252a30228254772aa242d31a32130482eaada2194da842782c3a394819",
		16: "e5be18092b2fb69bbe2b4be1ec08e0da1dccc5bf4131b7cb14d5ae7742412d4b",
		17: "ebd00325fa21cf6937cc4ec5045366708f6c9b2f00851874a350b8af34c33271",
		18: "6c9f4f7dab6a0bfcd4c140eb97bb70de7a635d1129c23bfdb3ebc0b2609188be",
		19: "cd79eaff85d3ac35d01633c599bae13f407bb20cbf77f9877cd2af14bd430cdf",
		20: "a6d54bcc34b108caa43de286645da7dadbb9dc68c2cf779bd7fceb945406713f",
		21: "7fe0fd7cdfc80d511b0af8f9c1f2eca4a40d7d6d78efd521d177ec2e85d449d1",
		22: "db6979d603b63e4b33f7e6de6df4125cc161df7a75c7e2230c5a283e7044bf62",
		23: "9bbd8c12dfc24a3d14ed575a7eb9c36639d54fba895b7536ef9bb9757f8a6218",
		24: "2a5ef3e5ccf099ab476612b84518ad6b63209c974ce38459e0eb6db2bc4aaa86",
		25: "39287ccc58a92e4e3f3d6d4437887f52cb609df2bb1ce2db326a9da612c12b5d",
	},
	"postgres": {
		1:  "21d634f2dd7b1675438cded1804a9ff2808dda0f1d10517a93f2a0422a80e1b9",
		2:  "54d5474eff99f479555e70be37f9d4518eb05e66d52e41fffcdd8225a0a5791d",
		3:  "ed52880e4fa1d40f98254ff466ed402a73759453dc29f79751020c3de426bcc3",
		4:  "df2b5aeb7e7759ce1a76e88bf2c994ed8464a32dc1af3186f51c65a627c46e1a",
		5:  "0f44e7e04d8d4869aebdbe2c12ee18f537b879cc3a49e88860093a408da24f00",
		6:  "38234a668118a6021a7eca4401952f72b063ac0277f2bafa2fec8e488aa4f1c4",
		7:  "f4d8155e18eceb74f461f989e7c4e31890757e4c72a647a4633018471c1028d6",
		8:  "5deada4fe6d5df263470afa2678793d7629544d72473477747be2b6d51bdf86e",
		9:  "b9e15d70ed4be9ee0e75415f424e913666a8b84a0ecd61b2b321d7f099962bd7",
		10: "9756597e6b1e95b85511691889888db292c968cd0bbc1661aa88e0c9c49489e1",
		11: "0846a6a385a5aa54eebcabb790b364dff487ffa4bbbfdd16c2d5c77c3b5e6044",
		12: "9e6f7dfa2adba65df32790918a1baf57b437342fefc72d90eb38c7577d4300d7",
		13: "0a53d06905eddcd2461153c263604114dbae38e206780826e71fc7757049a1d2",
		14: "72a259f0a5311123f5c00a7dd5597b521962d21c1ac9989482c8af09b04dadd1",
		15: "445df4c08c59ae2a8cd83d141ed8a35ca5a4c67a5b7aacbd876c359f150d816a",
		16: "c9ebb7a8f74a70bad7378e2b12ca2b987085c4e7b04d9daeabf48edb73289335",
		17: "1f2d42526de75310b5bf8e5bda1e88ebaaa99186eabc0a0081f635fc3e84c1fa",
		18: "a4810997af6ebea9c6c5979585fe099e9f152c182098f28f09d3502de239da44",
		19: "efe54fa7ffdba96d60e4d9ff240c2ee4214b3e55e3ec2e92f7297aafc2ff7d32",
		20: "5709b516a28984252fa5744abe5d22add142d1c80c305bafa9c6046e19ce8e99",
		21: "2734cb88402a4ee059b3171a967ed22085b698cc3942e23388af7dccbf9fbcfd",
		22: "7f51173e75fe61ff6d1ed336a9bc4a4bb5de4a1a0a123f8976fcbfcb529222b8",
		23: "877b825b38549f0c3147cb1fbea25fada814973ffa02a45bcc3f5441cab503ef",
		24: "417b67c85de12a016c355a14572c1de3f951946f8109767a7e6677c046847bb2",
		25: "98d879e4d6236e88dc2d027b4ce940127be168f91ee9fc36c2d5bdeea2d51de4",
	},
	"mysql": {
		1:  "c7fcf43f66c587e3ce22212f7bc6c5464b270394fff2d80bf064471dc424370f",
		2:  "35778d01f42c944a6645726e96292fb2dba72ae4de934bde81c888c8e782c40e",
		3:  "45983ae63f0a6246b316610dadb701abd5399d1b6ba880d442b3c1136592f946",
		4:  "cce18140643c420927fd8dbfe8679092ac457f8fcd4600e825bccf7c7d710244",
		5:  "0927db89cd8800dd2065a10a0626c469711eb436132e0295c71d5013bb54f0cb",
		6:  "9525394df7972f1291b3b480dd29c8da36057ed8c4c0198279df3b2a4277c3d9",
		7:  "1693a3f5fb6a5113e2fbb2b70f65cc9493228034030ee56b5978c5c8f35d2124",
		8:  "c10f8f1672077813d689ea5a48d906d33c80a5b12ad5ef3a2ed04dd56c78533c",
		9:  "ec0476dd3dfbcae55fd0ea657d763aed7aa40a8def60336ebfbc332c5b532881",
		10: "4fcd62d184615e4a97643126cd9580c7c3e41cf73da0263294d4f26b8155cf38",
		11: "b172e4c7ad4e83a1cf63244ec9711917f5d658de7ec3627e306ce2b714490293",
		12: "a53ef5e929ea3278beb93a5b9baf836059cc3a1b0a2387d89d53fc5bcddf356d",
		13: "e9caf7d8ffa11958f8d3bf801098eeb114b0b7e348297fdc2b6aa719d4bc8d6f",
		14: "5c18611ad3bff84f8e997d82a2779fb3b8d890b5e710d4f8f1dca08df6aaebc6",
		15: "2c50e001859c00f0bf55fd13a1eb03bcd3a83db8d05ac680bda12be507dffa77",
		16: "e0348cee8f3211954daca467ef92e3a19002e4ca6f825f64ca5b1125c0755d00",
		17: "e678c0c5b12a6487e3eab67e9a5e98911b8ccfe63ba15b3e8edf0edd90bc4ed3",
		18: "e37501d41e1117d10a12cf8447425a4a4f69b57d605d3b400626d9ef26198c7b",
		19: "30ff0d1c9d3646c677933bc1d8858e20d21172d984c3fbcdb2d5a417d8c69b18",
		20: "a9ad3eb5184111134032821b22d9bbc1e69dc7ee10c2b638b94ddfb18b07dd2c",
		21: "d70f762b81805c85d7290b60253c48f70b0f6de569762fe558d6dacb2420f83c",
		22: "99d915ab7bcb147930d61d446e3303f6d515a6dd8a946ed6f1d9ec53cf86335f",
		23: "c53bc76e4269b26ec532a91710acf9e73d90d859ec49a4064af4c199dfd8504e",
		24: "00b74a2c946b510944675ceb7e012023cf622b90c092ceb9a2ad96ec5c4b7065",
		25: "df1203409952b1a4df0b0e80bb70e9d45a380bfcb490187639f06aab838a6077",
	},
}

// TestReleasedMigrationChecksumsAreStable fails when a shipped migration's
// translated SQL changes, which is what a database already carrying that
// version rejects at startup.
func TestReleasedMigrationChecksumsAreStable(t *testing.T) {
	t.Parallel()

	for _, dialect := range []string{"sqlite", "postgres", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			t.Parallel()

			registry, err := registryForDialect(dialect)
			require.NoError(t, err)
			pinned := releasedMigrationChecksums[dialect]
			require.NotEmpty(t, pinned, "no pinned checksums for %s", dialect)

			for _, migration := range registry {
				want, ok := pinned[migration.Version]
				require.True(
					t,
					ok,
					"migration version %d (%s) has no pinned %s checksum;"+
						" add one in the change that adds the migration",
					migration.Version,
					migration.Name,
					dialect,
				)
				require.Equal(
					t,
					want,
					migration.checksum(),
					"%s migration %d (%s) changed after release;"+
						" add a new version instead of editing this one",
					dialect,
					migration.Version,
					migration.Name,
				)
			}
			require.Len(
				t,
				pinned,
				len(registry),
				"pinned %s checksums cover versions not in the registry",
				dialect,
			)
			for version := range pinned {
				require.LessOrEqual(
					t,
					version,
					len(registry),
					"pinned %s checksum for unknown version "+
						strconv.Itoa(version),
					dialect,
				)
			}
		})
	}
}
