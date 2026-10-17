
.cache/invalid-address-perf-20261017/builds/isolated-miss/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000054000 <clockcache_get>:
   54000:	push   %r15
   54002:	lea    -0x1(%rcx),%eax
   54005:	push   %r14
   54007:	push   %r13
   54009:	push   %r12
   5400b:	push   %rbp
   5400c:	push   %rbx
   5400d:	mov    %rsi,%rbx
   54010:	sub    $0x38,%rsp
   54014:	mov    %edx,0x8(%rsp)
   54018:	mov    %ecx,0xc(%rsp)
   5401c:	mov    %r8,0x18(%rsp)
   54021:	movq   $0x0,0x28(%rsp)
   5402a:	cmp    $0x7,%eax
   5402d:	ja     543f0 <clockcache_get+0x3f0>
   54033:	mov    0x8(%rdi),%rcx
   54037:	xor    %edx,%edx
   54039:	mov    %rdi,%r15
   5403c:	mov    0x8(%rcx),%rax
   54040:	mov    0x8(%rax),%rsi
   54044:	mov    %rbx,%rax
   54047:	div    %rsi
   5404a:	test   %rdx,%rdx
   5404d:	jne    5422c <clockcache_get+0x22c>
   54053:	xor    %r13d,%r13d
   54056:	cs nopw 0x0(%rax,%rax,1)
   54060:	mov    0x120(%rcx),%rcx
   54067:	mov    %rbx,%rax
   5406a:	shr    %cl,%rax
   5406d:	mov    0x28(%r15),%rcx
   54071:	shr    $0x2,%rcx
   54075:	cmp    %rcx,%rax
   54078:	jae    54288 <clockcache_get+0x288>
   5407e:	mov    0x30(%r15),%rdx
   54082:	mov    (%rdx,%rax,4),%ebp
   54085:	cmp    $0xffffffff,%ebp
   54088:	je     541e0 <clockcache_get+0x1e0>
   5408e:	data16 lea 0x81f12(%rip),%rdi        # d5fa8 <xxxtid@@Base+0xd5fa8>
   54096:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   5409e:	mov    0x8(%rsp),%r8d
   540a3:	mov    %ebp,%esi
   540a5:	mov    %r15,%rdi
   540a8:	mov    (%rax),%rax
   540ab:	mov    %rax,0x10(%rsp)
   540b0:	test   %r8d,%r8d
   540b3:	je     54270 <clockcache_get+0x270>
   540b9:	call   aa920 <clockcache_try_get_read.constprop.0>
   540be:	cmp    $0x1,%eax
   540c1:	jne    5410d <clockcache_get+0x10d>
   540c3:	mov    $0x1,%r14d
   540c9:	movabs $0xcccccccccccccccd,%rax
   540d3:	mul    %r14
   540d6:	xor    %eax,%eax
   540d8:	shr    $0x2,%rdx
   540dc:	mov    %rdx,%r12
   540df:	nop
   540e0:	pause
   540e2:	mov    %rax,%rcx
   540e5:	add    $0x1,%rax
   540e9:	cmp    %rcx,%r12
   540ec:	jne    540e0 <clockcache_get+0xe0>
   540ee:	cmp    $0x400,%r14
   540f5:	ja     541b8 <clockcache_get+0x1b8>
   540fb:	mov    %ebp,%esi
   540fd:	mov    %r15,%rdi
   54100:	add    %r14,%r14
   54103:	call   aa920 <clockcache_try_get_read.constprop.0>
   54108:	cmp    $0x1,%eax
   5410b:	je     540c9 <clockcache_get+0xc9>
   5410d:	test   %eax,%eax
   5410f:	jne    5420f <clockcache_get+0x20f>
   54115:	mov    %ebp,%eax
   54117:	mov    %rax,%r12
   5411a:	mov    %rax,%r14
   5411d:	mov    0x48(%r15),%rax
   54121:	shl    $0x6,%r12
   54125:	mov    0x8(%rax,%r12,1),%rdx
   5412a:	cmp    %rdx,%rbx
   5412d:	je     54144 <clockcache_get+0x144>
   5412f:	jmp    542e4 <clockcache_get+0x2e4>
   54134:	nopl   0x0(%rax)
   54138:	mov    %r15,%rdi
   5413b:	call   ea40 <clockcache_wait@plt>
   54140:	mov    0x48(%r15),%rax
   54144:	mov    0x10(%rax,%r12,1),%eax
   54149:	test   $0x10,%al
   5414b:	jne    54138 <clockcache_get+0x138>
   5414d:	add    0x48(%r15),%r12
   54151:	mov    0x14(%r12),%ecx
   54156:	test   %ecx,%ecx
   54158:	jne    54338 <clockcache_get+0x338>
   5415e:	mov    0x20(%r12),%eax
   54163:	cmp    %eax,0xc(%rsp)
   54167:	jne    543b8 <clockcache_get+0x3b8>
   5416d:	mov    0x8(%r15),%rax
   54171:	mov    0x18(%rax),%eax
   54174:	test   %eax,%eax
   54176:	jne    542b3 <clockcache_get+0x2b3>
   5417c:	mov    %r12,0x28(%rsp)
   54181:	test   %r13d,%r13d
   54184:	jne    5425a <clockcache_get+0x25a>
   5418a:	mov    0x28(%rsp),%rax
   5418f:	mov    0x18(%rsp),%rbx
   54194:	mov    %rax,(%rbx)
   54197:	data16 lea 0x81e09(%rip),%rdi        # d5fa8 <xxxtid@@Base+0xd5fa8>
   5419f:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   541a7:	mov    (%rax),%rax
   541aa:	mov    %rax,0x8(%rbx)
   541ae:	jmp    5425a <clockcache_get+0x25a>
   541b3:	nopl   0x0(%rax,%rax,1)
   541b8:	mov    %ebp,%esi
   541ba:	mov    %r15,%rdi
   541bd:	call   aa920 <clockcache_try_get_read.constprop.0>
   541c2:	cmp    $0x1,%eax
   541c5:	jne    5410d <clockcache_get+0x10d>
   541cb:	xor    %eax,%eax
   541cd:	nopl   (%rax)
   541d0:	pause
   541d2:	mov    %rax,%rdx
   541d5:	add    $0x1,%rax
   541d9:	cmp    %rdx,%r12
   541dc:	jne    541d0 <clockcache_get+0x1d0>
   541de:	jmp    541b8 <clockcache_get+0x1b8>
   541e0:	mov    0xc(%rsp),%ecx
   541e4:	mov    0x8(%rsp),%edx
   541e8:	lea    0x24(%rsp),%r9
   541ed:	mov    %rbx,%rsi
   541f0:	lea    0x28(%rsp),%r8
   541f5:	mov    %r15,%rdi
   541f8:	mov    %r13d,0x24(%rsp)
   541fd:	call   53c70 <clockcache_get_uncached>
   54202:	mov    0x24(%rsp),%r13d
   54207:	test   %eax,%eax
   54209:	je     54181 <clockcache_get+0x181>
   5420f:	mov    0x8(%r15),%rcx
   54213:	xor    %edx,%edx
   54215:	mov    0x8(%rcx),%rax
   54219:	mov    0x8(%rax),%rsi
   5421d:	mov    %rbx,%rax
   54220:	div    %rsi
   54223:	test   %rdx,%rdx
   54226:	je     54060 <clockcache_get+0x60>
   5422c:	mov    0x81d9d(%rip),%rbp        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   54233:	mov    %rsi,%rcx
   54236:	mov    %rbx,%rdx
   54239:	xor    %eax,%eax
   5423b:	lea    0x6506e(%rip),%rsi        # b92b0 <__func__.21+0xe20>
   54242:	mov    0x0(%rbp),%rdi
   54246:	call   fbf0 <fprintf@plt>
   5424b:	mov    0x0(%rbp),%rdi
   5424f:	call   fe90 <fflush@plt>
   54254:	mov    $0x16,%r13d
   5425a:	add    $0x38,%rsp
   5425e:	mov    %r13d,%eax
   54261:	pop    %rbx
   54262:	pop    %rbp
   54263:	pop    %r12
   54265:	pop    %r13
   54267:	pop    %r14
   54269:	pop    %r15
   5426b:	ret
   5426c:	nopl   0x0(%rax)
   54270:	call   aa920 <clockcache_try_get_read.constprop.0>
   54275:	cmp    $0x1,%eax
   54278:	je     542d9 <clockcache_get+0x2d9>
   5427a:	cmp    $0x2,%eax
   5427d:	je     5420f <clockcache_get+0x20f>
   5427f:	jmp    54115 <clockcache_get+0x115>
   54284:	nopl   0x0(%rax)
   54288:	mov    0x81d41(%rip),%rbp        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   5428f:	imul   %rsi,%rcx
   54293:	mov    %rbx,%rdx
   54296:	xor    %eax,%eax
   54298:	lea    0x65049(%rip),%rsi        # b92e8 <__func__.21+0xe58>
   5429f:	mov    0x0(%rbp),%rdi
   542a3:	call   fbf0 <fprintf@plt>
   542a8:	mov    0x0(%rbp),%rdi
   542ac:	call   fe90 <fflush@plt>
   542b1:	jmp    54254 <clockcache_get+0x254>
   542b3:	mov    0x10(%rsp),%rbx
   542b8:	mov    0xc(%rsp),%eax
   542bc:	lea    0x0(,%rbx,8),%rdx
   542c4:	sub    %rbx,%rdx
   542c7:	lea    0x220(%rax,%rdx,8),%rax
   542cf:	addq   $0x1,(%r15,%rax,8)
   542d4:	jmp    5417c <clockcache_get+0x17c>
   542d9:	mov    $0xb,%r13d
   542df:	jmp    5425a <clockcache_get+0x25a>
   542e4:	mov    0x8(%r15),%rcx
   542e8:	mov    %r14,%rax
   542eb:	xor    %edx,%edx
   542ed:	mov    0x10(%rsp),%r8
   542f2:	divq   0x140(%rcx)
   542f9:	and    $0x3,%r8d
   542fd:	shl    $0x6,%edx
   54300:	lea    (%rdx,%rax,1),%esi
   54303:	mov    0x130(%rcx),%eax
   54309:	mov    0x88(%r15),%rdx
   54310:	mov    $0xffffffff,%ecx
   54315:	imul   %r8,%rax
   54319:	add    %rsi,%rax
   5431c:	lock xadd %cx,(%rdx,%rax,2)
   54322:	test   %cx,%cx
   54325:	jne    5420f <clockcache_get+0x20f>
   5432b:	push   %rcx
   5432c:	push   %r8
   5432e:	push   %rsi
   5432f:	jmp    54389 <clockcache_get+0x389>
   54331:	nopl   0x0(%rax)
   54338:	mov    0x8(%r15),%rsi
   5433c:	mov    %r14,%rax
   5433f:	xor    %edx,%edx
   54341:	mov    0x10(%rsp),%r9
   54346:	divq   0x140(%rsi)
   5434d:	and    $0x3,%r9d
   54351:	shl    $0x6,%edx
   54354:	lea    (%rdx,%rax,1),%r8d
   54358:	mov    0x130(%rsi),%eax
   5435e:	mov    $0xffffffff,%esi
   54363:	mov    0x88(%r15),%rdx
   5436a:	imul   %r9,%rax
   5436e:	add    %r8,%rax
   54371:	lock xadd %si,(%rdx,%rax,2)
   54377:	test   %si,%si
   5437a:	je     54384 <clockcache_get+0x384>
   5437c:	mov    %ecx,%r13d
   5437f:	jmp    5425a <clockcache_get+0x25a>
   54384:	push   %rdx
   54385:	push   %r9
   54387:	push   %r8
   54389:	push   0x28(%rsp)
   5438d:	xor    %r9d,%r9d
   54390:	lea    0x64df1(%rip),%r8        # b9188 <__func__.21+0xcf8>
   54397:	xor    %eax,%eax
   54399:	lea    0x5e28e(%rip),%rcx        # b262e <_fini+0x13d2>
   543a0:	lea    0x6f669(%rip),%rdx        # c3a10 <__func__.28.lto_priv.1>
   543a7:	mov    $0x27c,%esi
   543ac:	lea    0x5e224(%rip),%rdi        # b25d7 <_fini+0x137b>
   543b3:	call   ee00 <platform_assert_false@plt>
   543b8:	mov    0xc(%rsp),%ebx
   543bc:	mov    %ebp,%r9d
   543bf:	lea    0x5e344(%rip),%r8        # b270a <_fini+0x14ae>
   543c6:	lea    0x5e354(%rip),%rcx        # b2721 <_fini+0x14c5>
   543cd:	lea    0x6f3dc(%rip),%rdx        # c37b0 <__func__.19>
   543d4:	mov    $0x8d1,%esi
   543d9:	lea    0x5e1f7(%rip),%rdi        # b25d7 <_fini+0x137b>
   543e0:	push   %rbx
   543e1:	push   %rax
   543e2:	xor    %eax,%eax
   543e4:	call   ee00 <platform_assert_false@plt>
   543e9:	nopl   0x0(%rax)
   543f0:	sub    $0x8,%rsp
   543f4:	lea    0x65245(%rip),%r8        # b9640 <__func__.21+0x11b0>
   543fb:	xor    %eax,%eax
   543fd:	push   %rsi
   543fe:	mov    0x1c(%rsp),%r9d
   54403:	lea    0x6525e(%rip),%rcx        # b9668 <__func__.21+0x11d8>
   5440a:	lea    0x6f48f(%rip),%rdx        # c38a0 <__func__.23.lto_priv.1>
   54411:	mov    $0x1ca,%esi
   54416:	lea    0x5e1ba(%rip),%rdi        # b25d7 <_fini+0x137b>
   5441d:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
