; ModuleID = 'test.c'
source_filename = "test.c"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

@.str = private unnamed_addr constant [6 x i8] c"test\0a\00", align 1
@.s1 = global  [4 x i8] c"%d\0a\00"
@.s2 = global [10 x i8] c"%s %s %s\0a\00"


@.str.0 = private unnamed_addr constant [4 x i8] c"157\00", align 1
@.str.1 = private unnamed_addr constant [3 x i8] c"vc\00", align 1
@.str.2 = private unnamed_addr constant [6 x i8] c"fdsvs\00", align 1
@.str.3 = private unnamed_addr constant [5 x i8] c"vcvd\00", align 1

@.arr.0 = global [4 x i8*] [i8* getelementptr inbounds ([4 x i8], [4 x i8]* @.str.0, i32 0, i32 0), i8* getelementptr inbounds ([3 x i8], [3 x i8]* @.str.1, i32 0, i32 0), i8* getelementptr inbounds ([6 x i8], [6 x i8]* @.str.2, i32 0, i32 0), i8* getelementptr inbounds ([5 x i8], [5 x i8]* @.str.3, i32 0, i32 0)]
define i32 @consume(i8** %arg) {
    %first = load i8*, i8** %arg;
    %second = getelementptr i8*, i8** %arg, i64 1
    %third = getelementptr i8*, i8** %arg, i64 2

    %second1 = load i8*, i8** %second;
    %third1 = load i8*, i8** %third;

    %cloc = getelementptr [10 x i8], [10 x i8]* @.s2, i64 0, i64 0;
    call i32 (i8*, ...) @printf(i8* %cloc, i8* %first, i8* %second1, i8* %third1);
    ret i32 0;
}

define i32 @do_query(i8** %tuple) {
    %id = getelementptr i8*, i8** %tuple, i32 0;
    %tele = getelementptr i8*, i8** %tuple, i32 1;

    %id.1 = load i8*, i8** %id;
    %tele.1 = load i8*, i8** %tele;
    %id.num = call i32(i8*, i8**, i32) @strtol (i8* %id.1, i8** null, i32 10);
    %tele.num = call i32(i8*, i8**, i32) @strtol (i8* %tele.1, i8** null, i32 10);

    %id.cmp = mul i32 %id.num, 3;
    %tele.cmp = add i32 %tele.num, 3;

    %cmp = icmp sge i32 %id.cmp, %tele.cmp;
    %cmp.1 = zext i1 %cmp to i32;
    ret i32 %cmp.1;
}

declare dso_local i32 @printf(i8*, ...) #1
declare dso_local i32 @strcmp(i8*, i8*)
declare dso_local i32 @strtol(i8*, i8**, i32)
!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"Ubuntu clang version 12.0.0-3ubuntu1~21.04.1"}
