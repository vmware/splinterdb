
.cache/invalid-address-perf-20261017/builds/mask-main/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000053fd0 <clockcache_get>:
   53fd0:	push   %r15
   53fd2:	lea    -0x1(%rcx),%eax
   53fd5:	push   %r14
   53fd7:	push   %r13
   53fd9:	push   %r12
   53fdb:	push   %rbp
   53fdc:	mov    %rsi,%rbp
   53fdf:	push   %rbx
   53fe0:	sub    $0x38,%rsp
   53fe4:	mov    %ecx,0x4(%rsp)
   53fe8:	mov    %r8,0x10(%rsp)
   53fed:	cmp    $0x7,%eax
   53ff0:	ja     54448 <clockcache_get+0x478>
   53ff6:	mov    %rdi,%r15
   53ff9:	mov    %edx,%r12d
   53ffc:	mov    0x8(%r15),%rax
   54000:	mov    0x8(%rax),%rdx
   54004:	mov    0x8(%rdx),%r14
   54008:	lea    -0x1(%r14),%rdx
   5400c:	test   %rbp,%rdx
   5400f:	jne    544ae <clockcache_get+0x4de>
   54015:	mov    0x120(%rax),%rcx
   5401c:	mov    0x28(%r15),%rax
   54020:	mov    %rbp,%rdx
   54023:	shr    %cl,%rdx
   54026:	shr    $0x2,%rax
   5402a:	cmp    %rax,%rdx
   5402d:	jae    5447a <clockcache_get+0x4aa>
   54033:	mov    0x30(%r15),%rax
   54037:	mov    (%rax,%rdx,4),%ebx
   5403a:	cmp    $0xffffffff,%ebx
   5403d:	jne    54060 <clockcache_get+0x90>
   5403f:	test   %r12d,%r12d
   54042:	jne    541b0 <clockcache_get+0x1e0>
   54048:	mov    $0xb,%r13d
   5404e:	add    $0x38,%rsp
   54052:	mov    %r13d,%eax
   54055:	pop    %rbx
   54056:	pop    %rbp
   54057:	pop    %r12
   54059:	pop    %r13
   5405b:	pop    %r14
   5405d:	pop    %r15
   5405f:	ret
   54060:	data16 lea 0x80f40(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   54068:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   54070:	mov    %ebx,%esi
   54072:	mov    %r15,%rdi
   54075:	mov    (%rax),%rax
   54078:	mov    %rax,0x8(%rsp)
   5407d:	test   %r12d,%r12d
   54080:	je     542c0 <clockcache_get+0x2f0>
   54086:	call   aa320 <clockcache_try_get_read.constprop.0>
   5408b:	cmp    $0x1,%eax
   5408e:	jne    540dd <clockcache_get+0x10d>
   54090:	mov    $0x1,%r13d
   54096:	movabs $0xcccccccccccccccd,%rax
   540a0:	mul    %r13
   540a3:	xor    %eax,%eax
   540a5:	shr    $0x2,%rdx
   540a9:	mov    %rdx,%r14
   540ac:	nopl   0x0(%rax)
   540b0:	pause
   540b2:	mov    %rax,%rcx
   540b5:	add    $0x1,%rax
   540b9:	cmp    %rcx,%r14
   540bc:	jne    540b0 <clockcache_get+0xe0>
   540be:	cmp    $0x400,%r13
   540c5:	ja     54180 <clockcache_get+0x1b0>
   540cb:	mov    %ebx,%esi
   540cd:	mov    %r15,%rdi
   540d0:	add    %r13,%r13
   540d3:	call   aa320 <clockcache_try_get_read.constprop.0>
   540d8:	cmp    $0x1,%eax
   540db:	je     54096 <clockcache_get+0xc6>
   540dd:	test   %eax,%eax
   540df:	jne    53ffc <clockcache_get+0x2c>
   540e5:	mov    %ebx,%eax
   540e7:	mov    0x48(%r15),%rcx
   540eb:	mov    %rax,%r14
   540ee:	shl    $0x6,%r14
   540f2:	mov    0x8(%rcx,%r14,1),%rdx
   540f7:	cmp    %rdx,%rbp
   540fa:	jne    543bc <clockcache_get+0x3ec>
   54100:	mov    %rax,%rbp
   54103:	jmp    54114 <clockcache_get+0x144>
   54105:	nopl   (%rax)
   54108:	mov    %r15,%rdi
   5410b:	call   ea40 <clockcache_wait@plt>
   54110:	mov    0x48(%r15),%rcx
   54114:	mov    0x10(%rcx,%r14,1),%eax
   54119:	test   $0x10,%al
   5411b:	jne    54108 <clockcache_get+0x138>
   5411d:	mov    0x48(%r15),%rdx
   54121:	mov    %rbp,%rax
   54124:	add    %r14,%rdx
   54127:	mov    0x14(%rdx),%r13d
   5412b:	test   %r13d,%r13d
   5412e:	jne    54408 <clockcache_get+0x438>
   54134:	mov    0x20(%rdx),%eax
   54137:	cmp    %eax,0x4(%rsp)
   5413b:	jne    54411 <clockcache_get+0x441>
   54141:	mov    0x8(%r15),%rax
   54145:	mov    0x18(%rax),%esi
   54148:	test   %esi,%esi
   5414a:	jne    54396 <clockcache_get+0x3c6>
   54150:	mov    %rdx,%r12
   54153:	mov    0x10(%rsp),%rbx
   54158:	xor    %r13d,%r13d
   5415b:	mov    %r12,(%rbx)
   5415e:	data16 lea 0x80e42(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   54166:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   5416e:	mov    (%rax),%rax
   54171:	mov    %rax,0x8(%rbx)
   54175:	jmp    5404e <clockcache_get+0x7e>
   5417a:	nopw   0x0(%rax,%rax,1)
   54180:	mov    %ebx,%esi
   54182:	mov    %r15,%rdi
   54185:	call   aa320 <clockcache_try_get_read.constprop.0>
   5418a:	cmp    $0x1,%eax
   5418d:	jne    540dd <clockcache_get+0x10d>
   54193:	xor    %eax,%eax
   54195:	data16 cs nopw 0x0(%rax,%rax,1)
   541a0:	pause
   541a2:	mov    %rax,%rdx
   541a5:	add    $0x1,%rax
   541a9:	cmp    %rdx,%r14
   541ac:	jne    541a0 <clockcache_get+0x1d0>
   541ae:	jmp    54180 <clockcache_get+0x1b0>
   541b0:	data16 lea 0x80df0(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   541b8:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   541c0:	mov    0x4(%rsp),%edx
   541c4:	mov    %rbp,%rsi
   541c7:	mov    %r15,%rdi
   541ca:	mov    (%rax),%r13
   541cd:	call   53d00 <clockcache_acquire_entry_for_load.lto_priv.0>
   541d2:	mov    %rax,%rbx
   541d5:	mov    $0xffffffff,%eax
   541da:	cmp    %rax,%rbx
   541dd:	je     53ffc <clockcache_get+0x2c>
   541e3:	mov    0x8(%r15),%rax
   541e7:	mov    %rbx,%r12
   541ea:	mov    %r13,0x8(%rsp)
   541ef:	shl    $0x6,%r12
   541f3:	add    0x48(%r15),%r12
   541f7:	mov    0x18(%rax),%ecx
   541fa:	test   %ecx,%ecx
   541fc:	jne    5436f <clockcache_get+0x39f>
   54202:	mov    0x18(%r15),%rdi
   54206:	mov    (%r12),%rsi
   5420a:	mov    %rbp,%rcx
   5420d:	mov    %r14,%rdx
   54210:	mov    (%rdi),%rax
   54213:	call   *(%rax)
   54215:	mov    %eax,%r13d
   54218:	test   %eax,%eax
   5421a:	jne    54339 <clockcache_get+0x369>
   54220:	mov    0x8(%r15),%rax
   54224:	mov    0x18(%rax),%edx
   54227:	test   %edx,%edx
   54229:	jne    542dc <clockcache_get+0x30c>
   5422f:	mov    %r13d,%ecx
   54232:	mov    %ebx,%edx
   54234:	mov    %rbp,%rsi
   54237:	mov    %r15,%rdi
   5423a:	call   52640 <clockcache_finish_load.lto_priv.0>
   5423f:	test   %r13d,%r13d
   54242:	je     54153 <clockcache_get+0x183>
   54248:	mov    0x8(%r15),%rcx
   5424c:	mov    %rbx,%rax
   5424f:	xor    %edx,%edx
   54251:	mov    0x8(%rsp),%rdi
   54256:	divq   0x140(%rcx)
   5425d:	and    $0x3,%edi
   54260:	shl    $0x6,%edx
   54263:	lea    (%rdx,%rax,1),%esi
   54266:	mov    0x130(%rcx),%eax
   5426c:	mov    0x88(%r15),%rdx
   54273:	mov    $0xffffffff,%ecx
   54278:	imul   %rdi,%rax
   5427c:	add    %rsi,%rax
   5427f:	lock xadd %cx,(%rdx,%rax,2)
   54285:	test   %cx,%cx
   54288:	jne    5404e <clockcache_get+0x7e>
   5428e:	push   %rax
   5428f:	xor    %r9d,%r9d
   54292:	xor    %eax,%eax
   54294:	lea    0x63f65(%rip),%r8        # b8200 <__func__.21+0xd30>
   5429b:	push   %rdi
   5429c:	lea    0x5d381(%rip),%rcx        # b1624 <_fini+0xa08>
   542a3:	lea    0x6e706(%rip),%rdx        # c29b0 <__func__.28.lto_priv.1>
   542aa:	push   %rsi
   542ab:	lea    0x5d325(%rip),%rdi        # b15d7 <_fini+0x9bb>
   542b2:	mov    $0x261,%esi
   542b7:	push   0x20(%rsp)
   542bb:	call   ee00 <platform_assert_false@plt>
   542c0:	call   aa320 <clockcache_try_get_read.constprop.0>
   542c5:	cmp    $0x1,%eax
   542c8:	je     54048 <clockcache_get+0x78>
   542ce:	cmp    $0x2,%eax
   542d1:	je     53ffc <clockcache_get+0x2c>
   542d7:	jmp    540e5 <clockcache_get+0x115>
   542dc:	lea    0x20(%rsp),%rsi
   542e1:	mov    $0x1,%edi
   542e6:	call   e410 <clock_gettime@plt>
   542eb:	mov    0x18(%rsp),%rax
   542f0:	imul   $0x3b9aca00,0x20(%rsp),%rdx
   542f9:	add    0x28(%rsp),%rdx
   542fe:	sub    %rax,%rdx
   54301:	mov    0x8(%rsp),%rax
   54306:	lea    0x0(,%rax,8),%rcx
   5430e:	sub    %rax,%rcx
   54311:	mov    0x4(%rsp),%eax
   54315:	lea    (%rax,%rcx,8),%rax
   54319:	lea    (%r15,%rax,8),%rax
   5431d:	addq   $0x1,0x1148(%rax)
   54325:	addq   $0x1,0x1220(%rax)
   5432d:	add    %rdx,0x1190(%rax)
   54334:	jmp    5422f <clockcache_get+0x25f>
   54339:	mov    %eax,%edi
   5433b:	call   efe0 <strerror@plt>
   54340:	mov    0x80c89(%rip),%r14        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   54347:	mov    0x4(%rsp),%ecx
   5434b:	mov    %rbp,%rdx
   5434e:	mov    %rax,%r8
   54351:	lea    0x64610(%rip),%rsi        # b8968 <__func__.21+0x1498>
   54358:	xor    %eax,%eax
   5435a:	mov    (%r14),%rdi
   5435d:	call   fbf0 <fprintf@plt>
   54362:	mov    (%r14),%rdi
   54365:	call   fe90 <fflush@plt>
   5436a:	jmp    54220 <clockcache_get+0x250>
   5436f:	lea    0x20(%rsp),%rsi
   54374:	mov    $0x1,%edi
   54379:	call   e410 <clock_gettime@plt>
   5437e:	imul   $0x3b9aca00,0x20(%rsp),%rax
   54387:	add    0x28(%rsp),%rax
   5438c:	mov    %rax,0x18(%rsp)
   54391:	jmp    54202 <clockcache_get+0x232>
   54396:	mov    0x8(%rsp),%rsi
   5439b:	mov    0x4(%rsp),%ecx
   5439f:	lea    0x0(,%rsi,8),%rax
   543a7:	sub    %rsi,%rax
   543aa:	lea    0x220(%rcx,%rax,8),%rax
   543b2:	addq   $0x1,(%r15,%rax,8)
   543b7:	jmp    54150 <clockcache_get+0x180>
   543bc:	mov    0x8(%r15),%rcx
   543c0:	xor    %edx,%edx
   543c2:	mov    0x8(%rsp),%rdi
   543c7:	divq   0x140(%rcx)
   543ce:	and    $0x3,%edi
   543d1:	shl    $0x6,%edx
   543d4:	lea    (%rdx,%rax,1),%esi
   543d7:	mov    0x130(%rcx),%eax
   543dd:	mov    0x88(%r15),%rdx
   543e4:	mov    $0xffffffff,%ecx
   543e9:	imul   %rdi,%rax
   543ed:	add    %rsi,%rax
   543f0:	lock xadd %cx,(%rdx,%rax,2)
   543f6:	test   %cx,%cx
   543f9:	jne    53ffc <clockcache_get+0x2c>
   543ff:	jmp    5428e <clockcache_get+0x2be>
   54404:	nopl   0x0(%rax)
   54408:	mov    0x8(%r15),%rcx
   5440c:	jmp    5424f <clockcache_get+0x27f>
   54411:	mov    0x4(%rsp),%esi
   54415:	mov    %ebx,%r9d
   54418:	lea    0x5d361(%rip),%r8        # b1780 <_fini+0xb64>
   5441f:	lea    0x5d371(%rip),%rcx        # b1797 <_fini+0xb7b>
   54426:	lea    0x6e3a3(%rip),%rdx        # c27d0 <__func__.19>
   5442d:	lea    0x5d1a3(%rip),%rdi        # b15d7 <_fini+0x9bb>
   54434:	push   %rsi
   54435:	mov    $0x8b6,%esi
   5443a:	push   %rax
   5443b:	xor    %eax,%eax
   5443d:	call   ee00 <platform_assert_false@plt>
   54442:	nopw   0x0(%rax,%rax,1)
   54448:	sub    $0x8,%rsp
   5444c:	lea    0x6420d(%rip),%r8        # b8660 <__func__.21+0x1190>
   54453:	xor    %eax,%eax
   54455:	push   %rsi
   54456:	mov    0x14(%rsp),%r9d
   5445b:	lea    0x6403e(%rip),%rcx        # b84a0 <__func__.21+0xfd0>
   54462:	lea    0x6e337(%rip),%rdx        # c27a0 <__func__.23.lto_priv.1>
   54469:	mov    $0x1af,%esi
   5446e:	lea    0x5d162(%rip),%rdi        # b15d7 <_fini+0x9bb>
   54475:	call   ee00 <platform_assert_false@plt>
   5447a:	imul   %r14,%rax
   5447e:	sub    $0x8,%rsp
   54482:	mov    %rbp,%r9
   54485:	mov    $0x195,%esi
   5448a:	lea    0x63e07(%rip),%r8        # b8298 <__func__.21+0xdc8>
   54491:	lea    0x5d1ba(%rip),%rcx        # b1652 <_fini+0xa36>
   54498:	lea    0x6e4f1(%rip),%rdx        # c2990 <__func__.33.lto_priv.0>
   5449f:	lea    0x5d131(%rip),%rdi        # b15d7 <_fini+0x9bb>
   544a6:	push   %rax
   544a7:	xor    %eax,%eax
   544a9:	call   ee00 <platform_assert_false@plt>
   544ae:	sub    $0x8,%rsp
   544b2:	mov    %rbp,%r9
   544b5:	lea    0x63da4(%rip),%r8        # b8260 <__func__.21+0xd90>
   544bc:	xor    %eax,%eax
   544be:	push   %r14
   544c0:	lea    0x5d16d(%rip),%rcx        # b1634 <_fini+0xa18>
   544c7:	lea    0x6e4c2(%rip),%rdx        # c2990 <__func__.33.lto_priv.0>
   544ce:	mov    $0x18e,%esi
   544d3:	lea    0x5d0fd(%rip),%rdi        # b15d7 <_fini+0x9bb>
   544da:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
