
.cache/invalid-address-perf-20261017/builds/original/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000aa0a0 <clockcache_get_from_disk_async.constprop.0>:
   aa0a0:	mov    0x18(%rdi),%rax
   aa0a4:	cmp    $0x1,%rax
   aa0a8:	je     aa2d0 <clockcache_get_from_disk_async.constprop.0+0x230>
   aa0ae:	push   %r12
   aa0b0:	push   %rbp
   aa0b1:	push   %rbx
   aa0b2:	mov    %rdi,%rbx
   aa0b5:	sub    $0x10,%rsp
   aa0b9:	test   %rax,%rax
   aa0bc:	je     aa1d0 <clockcache_get_from_disk_async.constprop.0+0x130>
   aa0c2:	lea    0x80(%rdi),%rbp
   aa0c9:	mov    0x80(%rbx),%rax
   aa0d0:	mov    %rbp,%rdi
   aa0d3:	call   *0x8(%rax)
   aa0d6:	cmp    $0x1,%eax
   aa0d9:	jne    aa160 <clockcache_get_from_disk_async.constprop.0+0xc0>
   aa0df:	mov    0x80(%rbx),%rax
   aa0e6:	mov    %rbp,%rdi
   aa0e9:	call   *0x10(%rax)
   aa0ec:	mov    0x20(%rbx),%rdx
   aa0f0:	mov    %eax,0x70(%rbx)
   aa0f3:	mov    0x60(%rbx),%eax
   aa0f6:	shl    $0x6,%rax
   aa0fa:	add    0x48(%rdx),%rax
   aa0fe:	mov    0x10(%rax),%eax
   aa101:	test   $0x10,%al
   aa103:	jne    aa2b0 <clockcache_get_from_disk_async.constprop.0+0x210>
   aa109:	mov    0x70(%rbx),%edi
   aa10c:	test   %edi,%edi
   aa10e:	jne    aa278 <clockcache_get_from_disk_async.constprop.0+0x1d8>
   aa114:	mov    0x80(%rbx),%rax
   aa11b:	mov    %rbp,%rdi
   aa11e:	call   *0x20(%rax)
   aa121:	mov    0x20(%rbx),%rsi
   aa125:	mov    0x70(%rbx),%edi
   aa128:	mov    0x8(%rsi),%rcx
   aa12c:	test   %edi,%edi
   aa12e:	jne    aa2e0 <clockcache_get_from_disk_async.constprop.0+0x240>
   aa134:	mov    0x18(%rcx),%eax
   aa137:	test   %eax,%eax
   aa139:	jne    aa170 <clockcache_get_from_disk_async.constprop.0+0xd0>
   aa13b:	mov    0x68(%rbx),%rax
   aa13f:	mov    %rax,0x48(%rbx)
   aa143:	movl   $0x1,0x54(%rbx)
   aa14a:	movq   $0x1,0x18(%rbx)
   aa152:	add    $0x10,%rsp
   aa156:	mov    $0x1,%eax
   aa15b:	pop    %rbx
   aa15c:	pop    %rbp
   aa15d:	pop    %r12
   aa15f:	ret
   aa160:	add    $0x10,%rsp
   aa164:	xor    %eax,%eax
   aa166:	pop    %rbx
   aa167:	pop    %rbp
   aa168:	pop    %r12
   aa16a:	ret
   aa16b:	nopl   0x0(%rax,%rax,1)
   aa170:	mov    %rsp,%rsi
   aa173:	mov    $0x1,%edi
   aa178:	mov    0x480(%rbx),%rbp
   aa17f:	call   e410 <clock_gettime@plt>
   aa184:	mov    0x58(%rbx),%rcx
   aa188:	imul   $0x3b9aca00,(%rsp),%rdx
   aa190:	add    0x8(%rsp),%rdx
   aa195:	lea    0x0(,%rcx,8),%rax
   aa19d:	sub    %rbp,%rdx
   aa1a0:	sub    %rcx,%rax
   aa1a3:	mov    0x30(%rbx),%ecx
   aa1a6:	lea    (%rcx,%rax,8),%rcx
   aa1aa:	mov    0x20(%rbx),%rax
   aa1ae:	lea    (%rax,%rcx,8),%rax
   aa1b2:	addq   $0x1,0x1148(%rax)
   aa1ba:	addq   $0x1,0x1220(%rax)
   aa1c2:	add    %rdx,0x1190(%rax)
   aa1c9:	jmp    aa13b <clockcache_get_from_disk_async.constprop.0+0x9b>
   aa1ce:	xchg   %ax,%ax
   aa1d0:	mov    0x30(%rdi),%edx
   aa1d3:	mov    0x28(%rdi),%rsi
   aa1d7:	mov    0x20(%rdi),%rdi
   aa1db:	call   53840 <clockcache_acquire_entry_for_load.lto_priv.0>
   aa1e0:	mov    $0xffffffff,%edx
   aa1e5:	mov    %rax,0x60(%rbx)
   aa1e9:	cmp    %rdx,%rax
   aa1ec:	je     aa440 <clockcache_get_from_disk_async.constprop.0+0x3a0>
   aa1f2:	mov    0x20(%rbx),%rdx
   aa1f6:	shl    $0x6,%rax
   aa1fa:	mov    0x28(%rbx),%rcx
   aa1fe:	mov    %rbx,%r9
   aa201:	lea    0x80(%rbx),%rbp
   aa208:	mov    0x2bd21(%rip),%r8        # d5f30 <clockcache_get_from_disk_async_callback@@Base+0x81c00>
   aa20f:	mov    0x18(%rdx),%rsi
   aa213:	add    0x48(%rdx),%rax
   aa217:	mov    %rbp,%rdi
   aa21a:	xor    %edx,%edx
   aa21c:	mov    %rax,0x68(%rbx)
   aa220:	mov    (%rsi),%rax
   aa223:	call   *0x18(%rax)
   aa226:	mov    %eax,0x70(%rbx)
   aa229:	test   %eax,%eax
   aa22b:	jne    aa458 <clockcache_get_from_disk_async.constprop.0+0x3b8>
   aa231:	mov    0x68(%rbx),%rax
   aa235:	mov    %rbp,%rdi
   aa238:	mov    (%rax),%rsi
   aa23b:	mov    0x80(%rbx),%rax
   aa242:	call   *(%rax)
   aa244:	mov    %eax,0x70(%rbx)
   aa247:	test   %eax,%eax
   aa249:	jne    aa390 <clockcache_get_from_disk_async.constprop.0+0x2f0>
   aa24f:	mov    0x20(%rbx),%rax
   aa253:	mov    0x8(%rax),%rax
   aa257:	mov    0x18(%rax),%r8d
   aa25b:	test   %r8d,%r8d
   aa25e:	jne    aa360 <clockcache_get_from_disk_async.constprop.0+0x2c0>
   aa264:	lea    -0x1a2(%rip),%rax        # aa0c9 <clockcache_get_from_disk_async.constprop.0+0x29>
   aa26b:	mov    %rax,0x18(%rbx)
   aa26f:	jmp    aa0c9 <clockcache_get_from_disk_async.constprop.0+0x29>
   aa274:	nopl   0x0(%rax)
   aa278:	call   efe0 <strerror@plt>
   aa27d:	mov    0x2bd4c(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   aa284:	mov    0x60(%rbx),%rcx
   aa288:	lea    0x181d1(%rip),%rsi        # c2460 <__func__.21+0x9fd0>
   aa28f:	mov    0x28(%rbx),%rdx
   aa293:	mov    %rax,%r8
   aa296:	xor    %eax,%eax
   aa298:	mov    (%r12),%rdi
   aa29c:	call   fbf0 <fprintf@plt>
   aa2a1:	mov    (%r12),%rdi
   aa2a5:	call   fe90 <fflush@plt>
   aa2aa:	jmp    aa114 <clockcache_get_from_disk_async.constprop.0+0x74>
   aa2af:	nop
   aa2b0:	mov    0x70(%rbx),%ecx
   aa2b3:	mov    0x28(%rbx),%rsi
   aa2b7:	mov    0x20(%rbx),%rdi
   aa2bb:	mov    0x60(%rbx),%edx
   aa2be:	call   539d0 <clockcache_finish_load.lto_priv.0>
   aa2c3:	jmp    aa109 <clockcache_get_from_disk_async.constprop.0+0x69>
   aa2c8:	nopl   0x0(%rax,%rax,1)
   aa2d0:	mov    $0x1,%eax
   aa2d5:	ret
   aa2d6:	cs nopw 0x0(%rax,%rax,1)
   aa2e0:	mov    0x70(%rbx),%eax
   aa2e3:	xor    %edx,%edx
   aa2e5:	mov    0x58(%rbx),%rdi
   aa2e9:	mov    %eax,0x50(%rbx)
   aa2ec:	mov    0x60(%rbx),%eax
   aa2ef:	mov    %rdi,%r9
   aa2f2:	and    $0x3,%r9d
   aa2f6:	divq   0x140(%rcx)
   aa2fd:	shl    $0x6,%edx
   aa300:	lea    (%rdx,%rax,1),%r8d
   aa304:	mov    0x130(%rcx),%eax
   aa30a:	mov    $0xffffffff,%ecx
   aa30f:	mov    0x88(%rsi),%rdx
   aa316:	imul   %r9,%rax
   aa31a:	add    %r8,%rax
   aa31d:	lock xadd %cx,(%rdx,%rax,2)
   aa323:	test   %cx,%cx
   aa326:	jne    aa143 <clockcache_get_from_disk_async.constprop.0+0xa3>
   aa32c:	push   %rdx
   aa32d:	lea    0x82fa(%rip),%rcx        # b262e <_fini+0x1512>
   aa334:	mov    $0x27c,%esi
   aa339:	xor    %eax,%eax
   aa33b:	push   %r9
   aa33d:	lea    0x196ac(%rip),%rdx        # c39f0 <__func__.28.lto_priv.1>
   aa344:	xor    %r9d,%r9d
   aa347:	push   %r8
   aa349:	lea    0xee38(%rip),%r8        # b9188 <__func__.21+0xcf8>
   aa350:	push   %rdi
   aa351:	lea    0x827f(%rip),%rdi        # b25d7 <_fini+0x14bb>
   aa358:	call   ee00 <platform_assert_false@plt>
   aa35d:	nopl   (%rax)
   aa360:	mov    %rsp,%rsi
   aa363:	mov    $0x1,%edi
   aa368:	call   e410 <clock_gettime@plt>
   aa36d:	imul   $0x3b9aca00,(%rsp),%rax
   aa375:	add    0x8(%rsp),%rax
   aa37a:	mov    %rax,0x480(%rbx)
   aa381:	jmp    aa264 <clockcache_get_from_disk_async.constprop.0+0x1c4>
   aa386:	cs nopw 0x0(%rax,%rax,1)
   aa390:	mov    0x80(%rbx),%rax
   aa397:	mov    %rbp,%rdi
   aa39a:	call   *0x20(%rax)
   aa39d:	mov    0x70(%rbx),%ecx
   aa3a0:	mov    0x28(%rbx),%rsi
   aa3a4:	mov    0x20(%rbx),%rdi
   aa3a8:	mov    0x60(%rbx),%edx
   aa3ab:	call   539d0 <clockcache_finish_load.lto_priv.0>
   aa3b0:	mov    0x70(%rbx),%eax
   aa3b3:	mov    0x20(%rbx),%rsi
   aa3b7:	xor    %edx,%edx
   aa3b9:	mov    0x58(%rbx),%rdi
   aa3bd:	mov    0x8(%rsi),%rcx
   aa3c1:	mov    %eax,0x50(%rbx)
   aa3c4:	mov    0x60(%rbx),%eax
   aa3c7:	mov    %rdi,%r9
   aa3ca:	and    $0x3,%r9d
   aa3ce:	divq   0x140(%rcx)
   aa3d5:	shl    $0x6,%edx
   aa3d8:	lea    (%rdx,%rax,1),%r8d
   aa3dc:	mov    0x130(%rcx),%eax
   aa3e2:	mov    $0xffffffff,%ecx
   aa3e7:	mov    0x88(%rsi),%rdx
   aa3ee:	imul   %r9,%rax
   aa3f2:	add    %r8,%rax
   aa3f5:	lock xadd %cx,(%rdx,%rax,2)
   aa3fb:	test   %cx,%cx
   aa3fe:	jne    aa143 <clockcache_get_from_disk_async.constprop.0+0xa3>
   aa404:	push   %r10
   aa406:	lea    0x8221(%rip),%rcx        # b262e <_fini+0x1512>
   aa40d:	mov    $0x27c,%esi
   aa412:	xor    %eax,%eax
   aa414:	push   %r9
   aa416:	lea    0x195d3(%rip),%rdx        # c39f0 <__func__.28.lto_priv.1>
   aa41d:	xor    %r9d,%r9d
   aa420:	push   %r8
   aa422:	lea    0xed5f(%rip),%r8        # b9188 <__func__.21+0xcf8>
   aa429:	push   %rdi
   aa42a:	lea    0x81a6(%rip),%rdi        # b25d7 <_fini+0x14bb>
   aa431:	call   ee00 <platform_assert_false@plt>
   aa436:	cs nopw 0x0(%rax,%rax,1)
   aa440:	movl   $0x0,0x54(%rbx)
   aa447:	movq   $0x1,0x18(%rbx)
   aa44f:	jmp    aa152 <clockcache_get_from_disk_async.constprop.0+0xb2>
   aa454:	nopl   0x0(%rax)
   aa458:	mov    0x28(%rbx),%rsi
   aa45c:	mov    0x20(%rbx),%rdi
   aa460:	mov    %eax,%ecx
   aa462:	mov    0x60(%rbx),%edx
   aa465:	call   539d0 <clockcache_finish_load.lto_priv.0>
   aa46a:	mov    0x70(%rbx),%eax
   aa46d:	mov    0x20(%rbx),%rsi
   aa471:	xor    %edx,%edx
   aa473:	mov    0x58(%rbx),%rdi
   aa477:	mov    0x8(%rsi),%rcx
   aa47b:	mov    %eax,0x50(%rbx)
   aa47e:	mov    0x60(%rbx),%eax
   aa481:	mov    %rdi,%r9
   aa484:	and    $0x3,%r9d
   aa488:	divq   0x140(%rcx)
   aa48f:	shl    $0x6,%edx
   aa492:	lea    (%rdx,%rax,1),%r8d
   aa496:	mov    0x130(%rcx),%eax
   aa49c:	mov    $0xffffffff,%ecx
   aa4a1:	mov    0x88(%rsi),%rdx
   aa4a8:	imul   %r9,%rax
   aa4ac:	add    %r8,%rax
   aa4af:	lock xadd %cx,(%rdx,%rax,2)
   aa4b5:	test   %cx,%cx
   aa4b8:	jne    aa143 <clockcache_get_from_disk_async.constprop.0+0xa3>
   aa4be:	push   %r11
   aa4c0:	lea    0x8167(%rip),%rcx        # b262e <_fini+0x1512>
   aa4c7:	mov    $0x27c,%esi
   aa4cc:	xor    %eax,%eax
   aa4ce:	push   %r9
   aa4d0:	lea    0x19519(%rip),%rdx        # c39f0 <__func__.28.lto_priv.1>
   aa4d7:	xor    %r9d,%r9d
   aa4da:	push   %r8
   aa4dc:	lea    0xeca5(%rip),%r8        # b9188 <__func__.21+0xcf8>
   aa4e3:	push   %rdi
   aa4e4:	lea    0x80ec(%rip),%rdi        # b25d7 <_fini+0x14bb>
   aa4eb:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
