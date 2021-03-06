import json
import mysql.connector


mydb = mysql.connector.connect(
	host="wikireviews-staging-db.codti60ftvmt.us-east-1.rds.amazonaws.com",
	user="root",
	passwd="mRlxUJMhQW3",
	database="wr_beta"

)

db = mydb.cursor()

def get_id_under_business():

	# get id of category under Business
	db.execute("select id from wr_beta.taxonomy_taxonomy where parent_id = %s", (115,))
	category_id = db.fetchall()

	print((category_id))
	queue = []
	for _id in category_id:
		queue.append(_id[0])

	res = []
	while len(queue) > 0:
		curr_id  = queue.pop()
		res.append(curr_id)

		db.execute("select id from wr_beta.taxonomy_taxonomy where parent_id = %s", (curr_id, ))
		list_child_id = db.fetchall()

		# no childs => continue
		if len(list_child_id) == 0:
			continue

		else:

			for child in list_child_id:
				print('child: ', child)
				queue.append(child[0])

		pass

	return res

def yelp_taxonomy_mapping():

	# hardcode the ids in order to save time	
	sql = """
		select a.id as 'taxonomy_id', category, parent_id, label, is_active, b.categories, b.business_info from taxonomy_taxonomy a 
		left join wr_beta.categories_US b 
		on a.category = b.categories 
		where a.id in (31355, 31368, 31365, 31367, 31366, 31364, 31363, 31362, 31361, 31360, 31359, 31358, 31357, 31356, 31048, 
		31083, 31082, 31081, 31080, 31079, 31078, 31074, 31077, 31076, 31075, 31073, 31069, 31072, 31071, 31070, 31068, 31067, 
		31066, 31065, 31064, 31063, 31062, 31059, 31061, 31060, 31058, 31057, 31056, 31055, 31054, 31053, 31052, 31051, 31050, 
		31049, 28392, 31297, 31296, 31295, 31294, 31286, 28401, 31293, 31292, 31291, 31290, 31289, 31288, 31287, 29475, 28409, 
		28408, 28407, 28406, 28405, 28404, 28403, 28402, 28400, 28399, 28398, 28397, 28396, 28395, 28394, 28393, 31285, 31284, 
		24559, 24364, 24367, 24366, 24365, 24360, 24363, 24362, 24361, 24353, 24359, 24358, 24357, 24356, 24355, 24354, 24344, 
		24352, 24351, 24350, 24349, 24348, 24347, 24346, 24345, 24331, 24343, 24342, 24341, 24340, 24339, 24338, 24337, 24336, 
		24335, 24334, 24333, 24332, 24299, 24330, 24329, 24328, 24327, 24326, 24325, 24324, 24323, 24322, 24321, 24320, 24319, 
		24318, 24317, 24316, 24315, 24314, 24313, 24312, 24311, 24310, 24309, 24308, 24307, 24306, 24305, 24304, 24303, 24302, 
		24301, 24300, 24237, 24298, 24297, 24296, 24295, 24294, 24293, 24292, 24291, 24290, 24289, 24288, 24287, 24286, 24285, 
		24284, 24283, 24282, 24281, 24280, 24279, 24278, 24277, 24276, 24275, 24274, 24273, 24272, 24271, 24270, 24269, 24268, 
		24267, 24266, 24265, 24264, 24263, 24262, 24261, 24260, 24259, 24258, 24257, 24256, 24255, 24254, 24253, 24252, 24251, 
		24250, 24249, 24248, 24247, 24246, 24245, 24244, 24243, 24242, 24241, 24240, 24239, 24238, 24223, 24236, 24235, 24234, 
		24233, 24232, 24231, 24230, 24229, 24228, 24227, 24226, 24225, 24224, 24209, 24222, 24221, 24220, 24219, 24218, 24217, 
		24216, 24215, 24214, 24213, 24212, 24211, 24210, 24205, 24208, 24207, 24206, 24201, 24204, 24203, 24202, 24194, 24200, 
		24199, 24198, 24197, 24196, 24195, 1187, 31498, 31497, 31496, 31495, 31494, 31493, 31492, 31491, 31490, 31487, 31486, 
		31485, 31484, 31483, 31482, 31481, 31480, 31479, 31478, 31477, 31476, 31475, 31474, 31473, 31472, 31471, 31470, 31469, 
		31468, 31467, 31452, 31451, 31450, 31449, 31448, 31445, 31447, 31446, 31444, 31443, 31442, 31429, 31441, 31440, 31438, 
		31439, 31437, 31436, 31435, 31434, 31433, 31432, 31431, 31430, 31427, 31426, 31425, 31424, 31423, 31422, 31415, 31421, 
		31420, 31419, 31418, 31417, 31416, 31414, 31413, 31412, 31411, 31403, 31410, 31409, 31408, 31407, 31406, 31405, 31404, 
		29487, 29480, 29479, 29478, 29477, 29476, 24193, 24192, 24191, 24190, 24189, 24188, 24187, 24186, 24185, 24184, 24183, 
		24182, 1232, 31598, 31597, 31596, 31595, 31594, 31593, 31488, 31489, 1234, 1233, 1231, 1230, 1229, 1228, 1227, 1226, 
		1225, 1224, 1223, 1222, 1221, 1220, 1219, 1218, 1217, 1216, 1215, 1214, 1213, 1212, 1211, 1210, 1209, 1208, 1207, 1206, 
		31466, 31465, 31464, 31463, 31462, 31461, 31459, 31460, 31457, 31458, 31456, 31455, 31454, 31453, 1205, 1204, 1203, 1202, 
		1201, 1200, 1199, 31428, 1198, 1197, 1196, 1195, 1194, 1193, 1192, 1191, 1190, 1189, 1188, 1172, 31578, 31564, 31563, 24181, 
		1181, 31588, 31587, 31586, 31585, 31584, 31583, 31582, 31581, 31580, 31579, 1186, 1185, 1184, 1183, 1182, 1180, 1179, 31592, 
		31591, 31590, 31589, 1178, 31577, 31576, 31575, 31574, 31573, 31572, 31571, 31570, 31569, 31568, 31567, 31566, 31565, 1177, 
		1176, 1175, 1174, 1173, 31562, 1125, 31561, 31559, 31560, 31558, 31557, 31556, 31555, 31554, 31553, 31552, 31551, 31550, 31549, 
		31548, 31547, 31546, 31545, 31544, 31543, 31542, 31541, 31540, 31539, 31515, 31514, 31513, 31512, 31511, 31510, 31509, 31508, 
		31507, 31506, 31505, 31504, 31503, 31502, 31501, 31500, 31499, 24147, 1169, 1171, 1170, 1168, 1167, 1166, 1165, 1164, 1163, 
		1162, 1161, 1160, 1159, 1158, 1157, 1156, 1155, 1154, 1153, 1152, 1151, 1150, 1149, 1148, 1147, 1146, 1145, 1144, 1143, 1142, 
		1141, 1140, 31538, 31537, 31536, 31535, 31534, 31533, 31532, 31525, 31531, 31530, 31529, 31528, 31527, 31526, 31522, 31524, 
		31523, 31521, 31520, 31519, 31518, 31517, 31516, 1139, 1138, 1137, 1136, 1135, 1134, 1133, 1132, 1131, 1130, 1129, 1128, 1127, 
		1126, 1031, 31369, 1036, 1035, 1034, 1033, 1032, 1017, 31354, 31353, 31352, 31351, 31350, 31349, 31348, 31347, 31346, 24180, 
		24179, 24178, 24177, 24176, 24175, 24174, 24173, 24172, 24171, 24170, 24169, 24168, 24167, 24166, 24165, 24164, 24163, 24162, 
		24161, 24160, 24159, 24158, 24157, 24156, 24155, 24154, 24153, 24152, 24151, 24150, 24149, 24148, 1026, 1030, 1029, 1028, 1027, 
		1025, 1024, 1023, 1022, 1021, 1020, 1019, 1018, 1000, 31344, 31345, 31343, 31342, 31341, 31340, 31339, 31338, 31337, 31336, 
		31335, 31334, 31333, 31332, 31331, 31330, 31327, 31329, 31328, 31326, 31325, 31324, 31323, 31322, 31321, 31320, 31319, 31318, 
		31317, 31315, 31316, 24146, 24145, 24144, 24143, 24142, 24141, 24140, 24139, 24138, 24137, 24136, 24135, 24134, 24133, 24132, 
		24131, 24130, 24129, 24128, 24127, 24126, 24125, 1016, 1015, 1014, 1013, 1012, 1011, 1010, 1009, 1008, 1007, 1006, 1005, 1004, 
		1003, 1002, 1001, 991, 31299, 31298, 999, 998, 31314, 31313, 31312, 997, 996, 995, 994, 993, 31311, 31310, 31309, 31308, 31307, 
		31306, 31305, 31304, 31303, 31302, 31301, 31300, 992, 970, 24124, 24123, 24122, 24121, 24120, 24119, 24118, 24117, 24116, 24115, 
		24114, 24113, 24112, 24111, 24110, 24108, 24107, 24106, 24105, 24104, 24103, 24102, 24101, 973, 972, 971, 945, 31283, 31282, 
		31281, 31280, 31279, 31278, 31273, 31277, 31276, 31275, 31274, 31272, 31271, 31270, 31269, 31268, 31267, 31264, 31266, 31265, 
		31263, 31262, 31258, 31257, 31256, 31255, 31254, 31253, 31250, 31249, 31248, 31247, 31246, 31245, 31244, 31243, 31240, 31239, 
		31238, 31237, 31236, 31235, 31234, 31233, 31232, 31231, 24100, 24099, 969, 968, 967, 966, 965, 964, 963, 962, 961, 960, 959, 
		958, 957, 31261, 956, 31260, 31259, 955, 954, 31252, 31251, 953, 952, 951, 950, 31242, 31241, 949, 948, 947, 946, 944, 24098, 
		24097, 24096, 24095, 24094, 24093, 24092, 24091, 24090, 24089, 24088, 24087, 24085, 24084, 24083, 24082, 24081, 24080, 24079, 
		24078, 24077, 24076, 24075, 24074, 24073, 24072, 24071, 24070, 24069, 24068, 24067, 24066, 24065, 24064, 24063, 24062, 24061, 
		24060, 24059, 24058, 24057, 24056, 24055, 24054, 24053, 24052, 24051, 24050, 24049, 24048, 24047, 24046, 927, 24390, 938, 943, 
		942, 941, 940, 939, 936, 937, 935, 934, 933, 932, 931, 930, 929, 928, 915, 31230, 31229, 31228, 31227, 926, 925, 924, 923, 
		922, 921, 920, 919, 918, 917, 916, 753, 31226, 31225, 31221, 31224, 31223, 31222, 31220, 31219, 31218, 31217, 31216, 31215, 
		31201, 31200, 31199, 31198, 31197, 31195, 31194, 31193, 31192, 31191, 31190, 31189, 31188, 31187, 31186, 31185, 31184, 31183, 
		29485, 29484, 29483, 29482, 29481, 24389, 24388, 24387, 24386, 24385, 24384, 24383, 24382, 24381, 24380, 24379, 24378, 24377, 
		24376, 24375, 24374, 24373, 908, 31212, 31211, 31210, 31209, 31208, 31207, 31206, 31205, 31204, 31203, 31202, 914, 913, 31214, 
		31213, 912, 911, 910, 909, 907, 906, 905, 904, 903, 902, 901, 900, 899, 898, 897, 896, 895, 894, 893, 892, 891, 890, 889, 888, 
		887, 886, 885, 884, 883, 882, 881, 880, 879, 878, 877, 876, 875, 874, 873, 872, 871, 870, 869, 868, 867, 866, 865, 864, 863, 
		862, 861, 860, 859, 858, 857, 856, 855, 854, 853, 852, 851, 850, 849, 848, 847, 846, 845, 844, 843, 842, 841, 840, 839, 838, 
		837, 836, 835, 834, 833, 832, 831, 830, 829, 828, 827, 826, 825, 824, 823, 31196, 822, 821, 820, 819, 818, 817, 816, 815, 814, 
		813, 812, 811, 810, 809, 808, 807, 806, 805, 804, 803, 802, 801, 800, 799, 798, 797, 796, 795, 794, 793, 792, 791, 790, 789, 
		788, 787, 786, 785, 784, 783, 782, 781, 780, 779, 778, 777, 776, 775, 774, 773, 772, 771, 770, 769, 768, 767, 766, 765, 764, 
		763, 762, 761, 760, 759, 758, 757, 756, 755, 754, 738, 752, 751, 750, 749, 748, 747, 746, 745, 744, 743, 742, 741, 740, 739, 
		539, 31182, 31181, 31180, 31179, 31178, 31177, 31176, 31175, 31174, 31173, 31172, 31171, 31170, 31169, 31168, 31167, 31165, 
		31166, 31164, 31163, 31162, 31161, 31160, 31159, 31158, 31157, 31156, 31155, 31154, 31153, 31152, 31151, 31136, 31135, 31132, 
		31134, 31133, 31131, 31128, 31127, 31126, 31124, 31123, 31122, 31121, 31120, 28419, 24372, 24371, 24370, 24369, 24368, 727, 
		737, 736, 735, 734, 733, 732, 731, 730, 729, 728, 601, 31150, 31149, 31148, 31147, 31146, 31144, 31143, 31142, 31141, 31140, 
		31139, 31138, 31137, 726, 725, 724, 723, 722, 721, 720, 719, 718, 717, 716, 715, 714, 713, 712, 711, 710, 709, 708, 707, 706, 
		705, 704, 703, 702, 701, 700, 699, 698, 697, 696, 695, 694, 693, 692, 691, 690, 689, 688, 687, 686, 685, 684, 683, 682, 681, 
		680, 679, 678, 677, 676, 675, 674, 673, 672, 31145, 671, 670, 669, 668, 667, 666, 665, 664, 663, 662, 661, 660, 659, 658, 657, 
		656, 655, 654, 653, 652, 651, 650, 649, 648, 647, 646, 645, 644, 643, 642, 641, 640, 639, 638, 637, 636, 635, 634, 633, 632, 
		631, 630, 629, 628, 627, 626, 625, 624, 623, 622, 621, 620, 619, 618, 617, 616, 615, 614, 613, 612, 611, 610, 609, 608, 
		607, 606, 605, 604, 603, 602, 589, 600, 599, 598, 597, 596, 595, 594, 593, 592, 591, 590, 544, 31118, 31119, 588, 587, 
		586, 585, 584, 583, 582, 581, 580, 579, 578, 577, 576, 575, 574, 573, 572, 571, 570, 569, 568, 567, 566, 565, 564, 563, 
		562, 561, 560, 559, 558, 557, 556, 555, 554, 553, 552, 551, 550, 549, 548, 547, 546, 545, 543, 31130, 31129, 542, 541, 
		31125, 540, 509, 31116, 31111, 31110, 31109, 31108, 31107, 31106, 31105, 31104, 31103, 31102, 31101, 31100, 31099, 31098, 
		31097, 31096, 31095, 31094, 31093, 31092, 31090, 31089, 29474, 29471, 29470, 29469, 29468, 529, 31115, 31114, 31113, 31112, 
		538, 537, 536, 535, 534, 533, 532, 531, 530, 528, 31117, 527, 526, 525, 524, 523, 522, 521, 520, 519, 518, 517, 516, 515, 
		514, 513, 31091, 512, 511, 510, 503, 31088, 31087, 31086, 31085, 31084, 24045, 24044, 24043, 24042, 24041, 24040, 24039, 
		24038, 24037, 24036, 24035, 24034, 24033, 24032, 24031, 24030, 24029, 24028, 24027, 24026, 24025, 24024, 24023, 508, 507, 
		506, 505, 504, 481, 501, 502, 500, 499, 498, 497, 496, 495, 494, 493, 492, 491, 490, 489, 488, 487, 486, 485, 484, 483, 
		482, 459, 31047, 31044, 31046, 31045, 31027, 31026, 31025, 31023, 31024, 28412, 24022, 24021, 24020, 24019, 24018, 471, 
		31043, 31042, 31041, 31040, 31039, 31038, 31037, 31036, 31035, 31034, 31033, 31032, 31031, 31030, 31029, 31028, 480, 479, 
		478, 477, 476, 475, 474, 473, 472, 470, 469, 468, 467, 466, 465, 464, 463, 462, 461, 460, 443, 31022, 31021, 31020, 31017, 
		31007, 31006, 31005, 31004, 458, 457, 456, 455, 454, 31019, 453, 31018, 452, 451, 450, 31016, 31015, 31014, 31013, 31012, 449, 
		448, 31011, 31010, 31009, 31008, 447, 446, 445, 444, 425, 31003, 31002, 31001, 31000, 30999, 30998, 30997, 30996, 30995, 
		30994, 30993, 30992, 30991, 30990, 30989, 30988, 30987, 30986, 30985, 30984, 30983, 30982, 30981, 30980, 30979, 30978, 30977, 
		30976, 30975, 30974, 30973, 30972, 30971, 30970, 30968, 30966, 30965, 30964, 29486, 24017, 24016, 24015, 442, 441, 440, 439, 
		438, 437, 436, 435, 434, 433, 432, 431, 430, 429, 30969, 428, 427, 30967, 426, 296, 24392, 24391, 424, 423, 422, 421, 420, 
		419, 418, 417, 416, 415, 414, 413, 412, 411, 410, 409, 408, 407, 406, 405, 404, 403, 402, 401, 400, 399, 398, 397, 396, 395, 
		394, 393, 392, 391, 390, 389, 388, 387, 386, 385, 384, 383, 382, 381, 380, 379, 378, 377, 376, 375, 374, 373, 372, 371, 370, 
		369, 368, 367, 366, 365, 364, 363, 362, 361, 360, 359, 358, 357, 356, 355, 354, 353, 352, 351, 350, 349, 348, 347, 346, 345, 
		344, 343, 342, 341, 340, 339, 338, 337, 336, 335, 334, 333, 332, 331, 330, 329, 328, 327, 326, 325, 324, 323, 322, 321, 320, 
		319, 318, 317, 316, 315, 314, 313, 312, 311, 310, 309, 308, 307, 306, 305, 304, 303, 302, 301, 300, 299, 298, 297, 279, 30962, 
		30961, 30959, 30960, 30958, 30956, 30955, 30952, 30951, 30950, 30946, 30949, 30948, 30947, 30945, 30944, 30943, 30940, 30939, 
		24012, 24011, 24010, 24009, 24008, 295, 30963, 294, 293, 30957, 292, 291, 290, 289, 288, 287, 30954, 30953, 286, 285, 284, 
		30942, 30941, 283, 282, 281, 280, 264, 269, 278, 277, 276, 275, 274, 273, 272, 271, 270, 268, 267, 266, 265, 117, 31402, 31401, 31400, 
		31372, 29467, 29466, 29465, 29464, 29463, 29462, 29461, 29460, 29459, 29458, 29457, 29456, 29455, 29454, 29453, 29452, 29451, 29450, 
		29449, 29448, 29447, 29446, 29445, 29444, 29443, 29442, 29441, 29440, 29439, 29438, 29437, 29436, 29435, 29434, 29433, 24014, 24013, 
		1124, 1123, 1122, 1121, 1120, 1119, 1118, 1117, 1116, 1115, 1114, 1113, 1112, 1111, 1110, 1109, 1108, 1107, 1106, 1105, 1104, 1103, 
		1102, 1101, 1100, 1099, 1098, 1097, 1096, 1095, 1094, 31399, 31398, 1093, 31397, 1092, 31396, 1091, 1090, 1089, 31395, 31394, 31393, 
		1088, 1087, 1086, 31392, 31391, 31390, 31389, 31388, 1085, 31387, 31386, 31385, 31384, 1084, 1083, 1082, 1081, 1080, 1079, 1078, 1077, 
		1076, 1075, 1074, 1073, 1072, 31383, 1071, 1070, 1069, 1068, 1067, 1066, 1065, 1064, 1063, 
		1062, 1061, 1060, 31382, 31381, 31380, 31379, 31378, 1059, 1058, 1057, 31377, 31376, 31375, 31374, 1056, 1055, 1054, 
		31373, 1053, 1052, 1051, 1050, 1049, 1048, 1047, 1046, 1045, 1044, 1043, 1042, 1041, 1040, 1039, 31371, 31370, 1038, 116)

		"""
	cnt = 0
	try:
		db.execute(sql)
		records = db.fetchall()

		for record in records:
			# print("record:", record)
			obj = recordProcessor(record)
			keys = list(obj.keys())

			sql_insert_to_reviews_additionalattributes = """

			INSERT INTO wr_beta.reviews_additionalattributes(NAME,TYPE,required,label,placeholder,LENGTH,minval,maxval,regex,active,content_type_id,
			possible_choices,is_multiple_choice_attribute,business_owner_can_answer_flag) 
			VALUES(%s,'VARCHAR',0,%s,NULL,NULL,NULL,NULL,NULL,1,%s,NULL,0,1)

			"""
			for key in keys:
				
				# check whether business_info with current taxonomy id is existing or not
				sql_check_existing = "select * from wr_beta.reviews_additionalattributes where name = %s and content_type_id = %s"
				val_check_existing = (key, record[0])

				db.execute(sql_check_existing, val_check_existing)
				res = db.fetchall()

				if len(res) == 0:
					print(str(val_check_existing))
					cnt += 1

					#insert to reviews_additionalattributes
					val_insert_to_reviews_additionalattributes = (key, key, record[0])
					db.execute(sql_insert_to_reviews_additionalattributes, val_insert_to_reviews_additionalattributes)
					mydb.commit()

				# values of each key
				# traverse each value then insert it to reviews_additionalattributes_options table
				
				sql_get_id_reviews_additionalattributes = "select id from wr_beta.reviews_additionalattributes where name = %s and content_type_id = %s"
				val_get_id_reviews_additionalattributes = (key, record[0])

				try:
					db.execute(sql_get_id_reviews_additionalattributes, val_get_id_reviews_additionalattributes)
					r = db.fetchall()

					id_reviews_additionalattributes = r[0][0]

					values = obj[key]
					for value in values:

						# insert into reviews_additionalattributes_options
						# check record is existing in reviews_additionalattributes_options or not
						sql_check_existing_reviews_additionalattributes_options = """ select reviews_additionalattributes_id from wr_beta.reviews_additionalattributes_options 
						where reviews_additionalattributes_id = %s and option_value = %s
						"""
						val_check_existing_reviews_additionalattributes_options = (id_reviews_additionalattributes, value)

						try:
							db.execute(sql_check_existing_reviews_additionalattributes_options, val_check_existing_reviews_additionalattributes_options)
							isExisted = db.fetchall()

							if len(isExisted) == 0:
								sql_insert_additionalattributes_options = "INSERT INTO wr_beta.reviews_additionalattributes_options(reviews_additionalattributes_id,option_value,is_active,created_on,updated_on) VALUES(%s, %s,1,NOW(),NULL)"
								val_insert_additionalattributes_options = (id_reviews_additionalattributes, value)

								db.execute(sql_insert_additionalattributes_options, val_insert_additionalattributes_options)
								mydb.commit()

						except Exception as e:
							print(str(e), "id_reviews_additionalattributes = ", id_reviews_additionalattributes)
							# raise
						else:
							pass
						finally:
							pass

				except Exception as e:
					print(str(e))
					# raise
				else:
					pass
				finally:
					pass


		print("total number of cnt = ", cnt)
	except Exception as e:
		raise
	else:
		pass
	finally:
		pass

def recordProcessor(record):
	# record = (29462, 'Syrian', 117, 'Syrian', 1, 'Syrian', "{'Accepts Apple Pay': ['Yes', 'No'], 'Accepts Google Pay': ['Yes', 'No'], 'Alcohol': ['Beer & Wine Only', 'Full Bar', 'No'], 'Noise Level': ['Quiet', 'Average'], 'Takes Reservations': ['Yes', 'No'], 'Has TV': ['Yes', 'No'], 'Attire': ['Casual'], 'Gender Neutral Restrooms': ['Yes'], 'Good for Kids': ['Yes', 'No'], 'Good for Groups': ['Yes', 'No'], 'Wheelchair Accessible': ['Yes', 'No'], 'Good For Dancing': ['Yes', 'No'], 'Delivery': ['Yes', 'No'], 'Dogs Allowed': ['Yes', 'No'], 'Best Nights': ['Fri, Sat, Sun', 'Sat'], 'Coat Check': ['Yes', 'No'], 'Smoking': ['Outdoor Area / Patio Only'], 'Accepts Credit Cards': ['Yes', 'No'], 'Take-out': ['Yes'], 'Offers Military Discount': ['Yes'], 'Has Pool Table': ['No'], 'Outdoor Seating': ['Yes', 'No'], 'Good For Happy Hour': ['Yes', 'No'], 'Waiter Service': ['Yes', 'No'], 'Wi-Fi': ['Free', 'Paid', 'No'], 'Accepts Cryptocurrency': ['Yes', 'No'], 'Caters': ['Yes', 'No'], 'Bike Parking': ['Yes', 'No'], 'Parking': ['Garage', 'Valet, Street', 'Street', 'Street, Private Lot', 'Private Lot', 'Garage, Street', 'Valet, Street, Private Lot'], 'Good For': ['Brunch, Lunch', 'Breakfast, Lunch', 'Lunch, Dinner', 'Breakfast, Brunch, Lunch, Dessert', 'Dinner'], 'Ambience': ['Casual, Trendy, Classy', 'Trendy', 'Casual, Trendy, Intimate', 'Casual', 'Casual, Trendy', 'Hipster, Casual, Trendy, Classy'], 'Liked by Vegans': ['Yes'], 'Has Halal Options': ['Yes'], 'Liked by Vegetarians': ['Yes'], 'Has Gluten-free Options': ['Yes'], 'Has Kosher Options': ['Yes'], 'Open to All': ['Yes'], 'Has Dairy-free Options': ['Yes'], 'Has Soy-free Options': ['Yes']}")
	obj = eval(record[6])

	keys = list(obj.keys())
	print(keys)
	return obj

if __name__ == "__main__":
	print("START PROGRAM...")
	# list_id = get_id_under_business()
	# print(list_id)

	record = (29462, 'Syrian', 117, 'Syrian', 1, 'Syrian', "{'Accepts Apple Pay': ['Yes', 'No'], 'Accepts Google Pay': ['Yes', 'No'], 'Alcohol': ['Beer & Wine Only', 'Full Bar', 'No'], 'Noise Level': ['Quiet', 'Average'], 'Takes Reservations': ['Yes', 'No'], 'Has TV': ['Yes', 'No'], 'Attire': ['Casual'], 'Gender Neutral Restrooms': ['Yes'], 'Good for Kids': ['Yes', 'No'], 'Good for Groups': ['Yes', 'No'], 'Wheelchair Accessible': ['Yes', 'No'], 'Good For Dancing': ['Yes', 'No'], 'Delivery': ['Yes', 'No'], 'Dogs Allowed': ['Yes', 'No'], 'Best Nights': ['Fri, Sat, Sun', 'Sat'], 'Coat Check': ['Yes', 'No'], 'Smoking': ['Outdoor Area / Patio Only'], 'Accepts Credit Cards': ['Yes', 'No'], 'Take-out': ['Yes'], 'Offers Military Discount': ['Yes'], 'Has Pool Table': ['No'], 'Outdoor Seating': ['Yes', 'No'], 'Good For Happy Hour': ['Yes', 'No'], 'Waiter Service': ['Yes', 'No'], 'Wi-Fi': ['Free', 'Paid', 'No'], 'Accepts Cryptocurrency': ['Yes', 'No'], 'Caters': ['Yes', 'No'], 'Bike Parking': ['Yes', 'No'], 'Parking': ['Garage', 'Valet, Street', 'Street', 'Street, Private Lot', 'Private Lot', 'Garage, Street', 'Valet, Street, Private Lot'], 'Good For': ['Brunch, Lunch', 'Breakfast, Lunch', 'Lunch, Dinner', 'Breakfast, Brunch, Lunch, Dessert', 'Dinner'], 'Ambience': ['Casual, Trendy, Classy', 'Trendy', 'Casual, Trendy, Intimate', 'Casual', 'Casual, Trendy', 'Hipster, Casual, Trendy, Classy'], 'Liked by Vegans': ['Yes'], 'Has Halal Options': ['Yes'], 'Liked by Vegetarians': ['Yes'], 'Has Gluten-free Options': ['Yes'], 'Has Kosher Options': ['Yes'], 'Open to All': ['Yes'], 'Has Dairy-free Options': ['Yes'], 'Has Soy-free Options': ['Yes']}")
	recordProcessor(record)
	yelp_taxonomy_mapping()
	print("DONE GET yelp_taxonomy_mapping!!!")

